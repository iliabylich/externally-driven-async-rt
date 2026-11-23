use core::{
    mem::offset_of,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::{
    conversions::u32_to_usize,
    data::queue::{Popped, Queue},
    sys::ThreadId,
};

const UNCLAIMED: usize = 0;

#[repr(C)]
#[must_use]
pub(crate) struct WakeQueue<const N: usize> {
    owner: AtomicUsize,
    queue: Queue<N>,
    idxs: [u32; N],
    root_idx: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum Woken {
    Root,
    Task(u32),
}

#[must_use]
pub(crate) struct RootSlot(());

impl RootSlot {
    const fn new() -> Self {
        Self(())
    }
}

// SAFETY: `owner` is atomic, so concurrent `claim`s agree on a single owner thread. `idxs` and
// `root_idx` never change after `new`. `queue` is made of `Cell`s that only the owner thread
// touches: the crate calls `push`, `push_root`, `pop` and `is_empty` only through a `!Send`
// `Context` of the claiming loop, and `wake` asserts the owner thread before it enqueues anything.
unsafe impl<const N: usize> Sync for WakeQueue<N> {}

impl<const N: usize> WakeQueue<N> {
    pub(crate) const fn new() -> Self {
        const {
            assert!(N > 0, "wake queue capacity must not be zero");
            assert!(
                offset_of!(Self, root_idx)
                    == offset_of!(Self, idxs).wrapping_add(size_of::<[u32; N]>()),
                "the root idx must directly follow the task idxs"
            );
        }
        let mut idxs = [0; N];
        let mut rest: &mut [u32] = &mut idxs;
        let mut idx: u32 = 0;
        while let [first, tail @ ..] = rest {
            *first = idx;
            idx = idx.wrapping_add(1);
            rest = tail;
        }
        Self {
            owner: AtomicUsize::new(UNCLAIMED),
            queue: Queue::new(),
            idxs,
            root_idx: idx,
        }
    }

    pub(crate) fn claim(&self) -> Option<RootSlot> {
        self.owner
            .compare_exchange(UNCLAIMED, ThreadId::current().to_raw(), Relaxed, Relaxed)
            .ok()
            .map(|_| RootSlot::new())
    }

    pub(crate) fn waker(&'static self, idx: u32) -> Waker {
        let idx = u32_to_usize(idx);
        if idx >= N {
            unreachable!("slot {idx} is out of bounds");
        }
        let ptr = core::ptr::from_ref(self)
            .wrapping_byte_add(offset_of!(Self, idxs))
            .cast::<u32>()
            .wrapping_add(idx);
        // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer to a
        // task idx of a queue that is never freed, derived from a pointer to the whole queue.
        unsafe { Waker::from_raw(RawWaker::new(ptr.cast(), vtable::<N>())) }
    }

    pub(crate) fn root_waker(&'static self) -> Waker {
        let ptr = core::ptr::from_ref(self)
            .wrapping_byte_add(offset_of!(Self, root_idx))
            .cast::<u32>();
        // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer to the
        // root idx of a queue that is never freed, derived from a pointer to the whole queue.
        unsafe { Waker::from_raw(RawWaker::new(ptr.cast(), vtable::<N>())) }
    }

    pub(crate) fn push(&self, idx: u32) {
        self.queue.push(idx);
    }

    pub(crate) fn push_root(&self) {
        self.queue.push_extra();
    }

    pub(crate) fn pop(&self) -> Option<Woken> {
        match self.queue.pop() {
            None => None,
            Some(Popped::Extra) => Some(Woken::Root),
            Some(Popped::Slot(idx)) => Some(Woken::Task(idx)),
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn idx_by_waker_data(&'static self, waker: &Waker) -> u32 {
        (0..u32::try_from(N).unwrap())
            .find(|&idx| waker.data() == self.waker(idx).data())
            .expect("the waker comes from `waker`")
    }
}

fn vtable<const N: usize>() -> &'static RawWakerVTable {
    const { &RawWakerVTable::new(clone::<N>, wake::<N>, wake::<N>, drop_waker) }
}

fn clone<const N: usize>(ptr: *const ()) -> RawWaker {
    RawWaker::new(ptr, vtable::<N>())
}

unsafe fn wake<const N: usize>(ptr: *const ()) {
    let ptr = ptr.cast::<u32>();
    // SAFETY: `ptr` comes from `WakeQueue::waker` or `WakeQueue::root_waker`: it points to
    // `idxs[idx]`, which holds `idx`, or to `root_idx`, which holds `N` and directly follows
    // `idxs` (asserted in `new`), of a queue that is never freed, and carries the provenance of the
    // whole queue, so stepping back by `idx` idxs and by the offset of `idxs` lands on that queue.
    let (idx, queue) = unsafe {
        let idx = *ptr;
        let queue = ptr
            .sub(u32_to_usize(idx))
            .byte_sub(offset_of!(WakeQueue<N>, idxs))
            .cast::<WakeQueue<N>>();
        (idx, &*queue)
    };
    assert_eq!(
        queue.owner.load(Relaxed),
        ThreadId::current().to_raw(),
        "a Waker must only be woken on the thread of its EventLoop"
    );
    if idx == queue.root_idx {
        queue.push_root();
    } else {
        queue.push(idx);
    }
}

const fn drop_waker(_: *const ()) {}

#[cfg(test)]
mod tests {
    use super::{WakeQueue, Woken};
    use crate::testing::assert_yields;

    #[test]
    fn waking_pushes_the_slot() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        assert!(QUEUE.claim().is_some());
        let waker = QUEUE.waker(1);
        waker.wake_by_ref();
        assert_yields!(core::iter::from_fn(|| QUEUE.pop()), [Woken::Task(1)]);
        let clone = waker.clone();
        drop(waker);
        clone.wake();
        assert_yields!(core::iter::from_fn(|| QUEUE.pop()), [Woken::Task(1)]);
    }

    #[test]
    fn duplicate_wakes_queue_the_slot_once() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        assert!(QUEUE.claim().is_some());
        let waker = QUEUE.waker(0);
        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake();
        assert_yields!(core::iter::from_fn(|| QUEUE.pop()), [Woken::Task(0)]);
    }

    #[test]
    fn waking_after_the_queue_was_mutated() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        assert!(QUEUE.claim().is_some());
        let waker = QUEUE.waker(0);
        QUEUE.push(1);
        waker.wake_by_ref();
        assert_eq!(QUEUE.pop(), Some(Woken::Task(1)));
        waker.wake();
        assert_yields!(core::iter::from_fn(|| QUEUE.pop()), [Woken::Task(0)]);
    }

    #[test]
    fn dropping_does_not_wake() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        assert!(QUEUE.claim().is_some());
        let waker = QUEUE.waker(0);
        drop(waker.clone());
        drop(waker);
        assert!(QUEUE.is_empty());
    }

    #[test]
    fn claiming_twice_fails() {
        let queue = WakeQueue::<1>::new();
        assert!(queue.claim().is_some());
        assert!(queue.claim().is_none());
    }

    #[test]
    fn waking_the_root_queues_it_in_order() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        assert!(QUEUE.claim().is_some());
        let root = QUEUE.root_waker();
        QUEUE.push(1);
        root.wake_by_ref();
        QUEUE.push(0);
        let clone = root.clone();
        drop(root);
        clone.wake();
        assert_yields!(
            core::iter::from_fn(|| QUEUE.pop()),
            [Woken::Task(1), Woken::Root, Woken::Task(0)]
        );
    }
}
