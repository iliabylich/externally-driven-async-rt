use core::{
    cell::Cell,
    mem::offset_of,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::{
    const_unreachable::const_unreachable, conversions::u32_to_usize, helpers::element,
    sys::ThreadId,
};

const NONE: u32 = u32::MAX;
const UNCLAIMED: usize = 0;

const fn decode(slot: u32) -> Option<u32> {
    if slot == NONE {
        return None;
    }
    Some(slot)
}

#[repr(C)]
#[must_use]
pub(crate) struct WakeQueue<const N: usize> {
    owner: AtomicUsize,
    head: Cell<u32>,
    tail: Cell<u32>,
    links: [Link; N],
    root: Link,
}

pub(crate) struct Link {
    slot: u32,
    queued: Cell<bool>,
    next: Cell<u32>,
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

// SAFETY: `owner` is atomic, so concurrent `claim`s agree on a single owner thread. Every other
// field is a `Cell` that only the owner thread touches: the crate calls `push`, `push_root`, `pop`
// and `is_empty` only through a `!Send` `Context` of the claiming loop, and `wake` asserts the
// owner thread before it enqueues anything.
unsafe impl<const N: usize> Sync for WakeQueue<N> {}

impl<const N: usize> WakeQueue<N> {
    pub(crate) const fn new() -> Self {
        const {
            assert!(N > 0, "wake queue capacity must not be zero");
            assert!(
                N < 0xFFFF_FFFF,
                "wake queue capacity must be less than u32::MAX"
            );
            assert!(
                offset_of!(Self, root)
                    == offset_of!(Self, links).wrapping_add(size_of::<[Link; N]>()),
                "the root link must directly follow the task links"
            );
        }
        let mut links = [const {
            Link {
                slot: 0,
                queued: Cell::new(false),
                next: Cell::new(NONE),
            }
        }; N];
        let mut rest: &mut [Link] = &mut links;
        let mut slot: u32 = 0;
        while let [first, tail @ ..] = rest {
            first.slot = slot;
            slot = slot.wrapping_add(1);
            rest = tail;
        }
        Self {
            owner: AtomicUsize::new(UNCLAIMED),
            head: Cell::new(NONE),
            tail: Cell::new(NONE),
            links,
            root: Link {
                slot,
                queued: Cell::new(false),
                next: Cell::new(NONE),
            },
        }
    }

    pub(crate) fn claim(&self) -> Option<RootSlot> {
        self.owner
            .compare_exchange(UNCLAIMED, ThreadId::current().to_raw(), Relaxed, Relaxed)
            .ok()
            .map(|_| RootSlot::new())
    }

    pub(crate) fn waker(&'static self, slot: u32) -> Waker {
        let slot = u32_to_usize(slot);
        if slot >= N {
            const_unreachable!("slot {} is out of bounds", slot);
        }
        let link = core::ptr::from_ref(self)
            .wrapping_byte_add(offset_of!(Self, links))
            .cast::<Link>()
            .wrapping_add(slot);
        // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer to a
        // link of a queue that is never freed, derived from a pointer to the whole queue.
        unsafe { Waker::from_raw(RawWaker::new(link.cast(), vtable::<N>())) }
    }

    pub(crate) fn root_waker(&'static self) -> Waker {
        let link = core::ptr::from_ref(self)
            .wrapping_byte_add(offset_of!(Self, root))
            .cast::<Link>();
        // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer to the
        // root link of a queue that is never freed, derived from a pointer to the whole queue.
        unsafe { Waker::from_raw(RawWaker::new(link.cast(), vtable::<N>())) }
    }

    const fn task_link(&self, slot: u32) -> &Link {
        let Some(link) = element(&self.links, u32_to_usize(slot)) else {
            const_unreachable!("slot {} is out of bounds", slot);
        };
        link
    }

    const fn link(&self, slot: u32) -> &Link {
        if slot == self.root.slot {
            return &self.root;
        }
        self.task_link(slot)
    }

    const fn enqueue(&self, link: &Link) {
        if link.queued.get() {
            return;
        }
        let _ = link.queued.replace(true);
        let _ = link.next.replace(NONE);
        let slot = link.slot;
        match decode(self.tail.get()) {
            Some(tail) => {
                let _ = self.link(tail).next.replace(slot);
            }
            None => {
                let _ = self.head.replace(slot);
            }
        }
        let _ = self.tail.replace(slot);
    }

    pub(crate) const fn push(&self, slot: u32) {
        self.enqueue(self.task_link(slot));
    }

    pub(crate) const fn push_root(&self) {
        self.enqueue(&self.root);
    }

    pub(crate) const fn pop(&self) -> Option<Woken> {
        let Some(slot) = decode(self.head.get()) else {
            return None;
        };
        let link = self.link(slot);
        let next = link.next.get();
        let _ = self.head.replace(next);
        if decode(next).is_none() {
            let _ = self.tail.replace(NONE);
        }
        let _ = link.queued.replace(false);
        let _ = link.next.replace(NONE);
        if slot == self.root.slot {
            return Some(Woken::Root);
        }
        Some(Woken::Task(slot))
    }

    pub(crate) const fn is_empty(&self) -> bool {
        decode(self.head.get()).is_none()
    }

    #[cfg(test)]
    pub(crate) fn id_by_waker_data(&'static self, waker: &Waker) -> u32 {
        (0..u32::try_from(N).unwrap())
            .find(|&id| waker.data() == self.waker(id).data())
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
    let link = ptr.cast::<Link>();
    // SAFETY: `ptr` comes from `WakeQueue::waker` or `WakeQueue::root_waker`: it points to
    // `links[slot]`, or to `root` whose slot is `N` and which directly follows `links` (asserted
    // in `new`), of a queue that is never freed, and carries the provenance of the whole queue,
    // so stepping back by `slot` links and by the offset of `links` lands on that queue.
    let (slot, queue) = unsafe {
        let slot = (*link).slot;
        let queue = link
            .sub(u32_to_usize(slot))
            .byte_sub(offset_of!(WakeQueue<N>, links))
            .cast::<WakeQueue<N>>();
        (slot, &*queue)
    };
    assert_eq!(
        queue.owner.load(Relaxed),
        ThreadId::current().to_raw(),
        "a Waker must only be woken on the thread of its EventLoop"
    );
    queue.enqueue(queue.link(slot));
}

const fn drop_waker(_: *const ()) {}

#[cfg(test)]
mod tests {
    use super::{WakeQueue, Woken};
    use crate::testing::assert_yields;

    #[test]
    fn pops_in_push_order() {
        let queue = WakeQueue::<4>::new();
        for slot in [2, 0, 3] {
            queue.push(slot);
        }
        assert!(!queue.is_empty());
        assert_yields!(
            core::iter::from_fn(|| queue.pop()),
            [Woken::Task(2), Woken::Task(0), Woken::Task(3)]
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn a_queued_slot_is_pushed_once() {
        let queue = WakeQueue::<2>::new();
        queue.push(0);
        queue.push(1);
        queue.push(0);
        assert_yields!(
            core::iter::from_fn(|| queue.pop()),
            [Woken::Task(0), Woken::Task(1)]
        );
        queue.push(0);
        assert_yields!(core::iter::from_fn(|| queue.pop()), [Woken::Task(0)]);
    }

    #[test]
    fn re_push_during_draining_goes_to_the_back() {
        let queue = WakeQueue::<2>::new();
        queue.push(0);
        queue.push(1);
        assert_eq!(queue.pop(), Some(Woken::Task(0)));
        queue.push(0);
        assert_yields!(
            core::iter::from_fn(|| queue.pop()),
            [Woken::Task(1), Woken::Task(0)]
        );
    }

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
