mod link;
mod woken;

use core::{
    mem::offset_of,
    sync::atomic::{AtomicU32, AtomicUsize, Ordering::Relaxed},
    task::{RawWaker, RawWakerVTable, Waker},
};

use self::link::Link;
pub(crate) use self::woken::Woken;
use crate::{conversions::u32_to_usize, sys::ThreadId, tasks::RootSlot};

const NONE: u32 = u32::MAX;
const UNCLAIMED: usize = 0;

fn decode(slot: u32) -> Option<u32> {
    (slot != NONE).then_some(slot)
}

#[repr(C)]
#[must_use]
pub(crate) struct WakeQueue<const N: usize> {
    owner: AtomicUsize,
    head: AtomicU32,
    tail: AtomicU32,
    links: [Link; N],
    root: Link,
}

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
        let mut links = [const { Link::new(0) }; N];
        let mut rest: &mut [Link] = &mut links;
        let mut slot: u32 = 0;
        while let [first, tail @ ..] = rest {
            *first = Link::new(slot);
            slot = slot.wrapping_add(1);
            rest = tail;
        }
        Self {
            owner: AtomicUsize::new(UNCLAIMED),
            head: AtomicU32::new(NONE),
            tail: AtomicU32::new(NONE),
            links,
            root: Link::new(slot),
        }
    }

    pub(crate) fn claim(&self) -> Option<RootSlot> {
        self.owner
            .compare_exchange(UNCLAIMED, ThreadId::current().to_raw(), Relaxed, Relaxed)
            .ok()
            .map(|_| RootSlot::new())
    }

    pub(crate) fn waker(&'static self, slot: u32) -> Waker {
        if u32_to_usize(slot) >= N {
            unreachable!("slot {slot} is out of bounds");
        }
        let link = core::ptr::from_ref(self)
            .wrapping_byte_add(offset_of!(Self, links))
            .cast::<Link>()
            .wrapping_add(u32_to_usize(slot));
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

    fn task_link(&self, slot: u32) -> &Link {
        let Some(link) = self.links.get(u32_to_usize(slot)) else {
            unreachable!("slot {slot} is out of bounds");
        };
        link
    }

    fn link(&self, slot: u32) -> &Link {
        if slot == self.root.slot() {
            return &self.root;
        }
        self.task_link(slot)
    }

    fn enqueue(&self, link: &Link) {
        if link.is_queued() {
            return;
        }
        link.set_queued(true);
        link.set_next(NONE);
        let slot = link.slot();
        match decode(self.tail.load(Relaxed)) {
            Some(tail) => self.link(tail).set_next(slot),
            None => self.head.store(slot, Relaxed),
        }
        self.tail.store(slot, Relaxed);
    }

    pub(crate) fn push(&self, slot: u32) {
        self.enqueue(self.task_link(slot));
    }

    pub(crate) fn push_root(&self) {
        self.enqueue(&self.root);
    }

    pub(crate) fn pop(&self) -> Option<Woken> {
        let slot = decode(self.head.load(Relaxed))?;
        let link = self.link(slot);
        let next = link.next();
        self.head.store(next, Relaxed);
        if decode(next).is_none() {
            self.tail.store(NONE, Relaxed);
        }
        link.set_queued(false);
        link.set_next(NONE);
        if slot == self.root.slot() {
            return Some(Woken::Root);
        }
        Some(Woken::Task(slot))
    }

    pub(crate) fn is_empty(&self) -> bool {
        decode(self.head.load(Relaxed)).is_none()
    }

    #[cfg(test)]
    pub(crate) fn id_by_waker_data(&'static self, waker: &Waker) -> u32 {
        (0..u32::try_from(N).unwrap())
            .find(|&id| waker.data() == self.waker(id).data())
            .expect("the waker comes from `waker`")
    }

    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = Woken> {
        core::iter::from_fn(|| self.pop())
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
        let slot = (*link).slot();
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
    use crate::{
        tasks::{WakeQueue, Woken},
        testing::assert_yields,
    };

    #[test]
    fn pops_in_push_order() {
        let queue = WakeQueue::<4>::new();
        for slot in [2, 0, 3] {
            queue.push(slot);
        }
        assert!(!queue.is_empty());
        assert_yields!(
            queue.iter(),
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
        assert_yields!(queue.iter(), [Woken::Task(0), Woken::Task(1)]);
        queue.push(0);
        assert_yields!(queue.iter(), [Woken::Task(0)]);
    }

    #[test]
    fn re_push_during_draining_goes_to_the_back() {
        let queue = WakeQueue::<2>::new();
        queue.push(0);
        queue.push(1);
        assert_eq!(queue.pop(), Some(Woken::Task(0)));
        queue.push(0);
        assert_yields!(queue.iter(), [Woken::Task(1), Woken::Task(0)]);
    }

    #[test]
    fn waking_pushes_the_slot() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        assert!(QUEUE.claim().is_some());
        let waker = QUEUE.waker(1);
        waker.wake_by_ref();
        assert_yields!(QUEUE.iter(), [Woken::Task(1)]);
        let clone = waker.clone();
        drop(waker);
        clone.wake();
        assert_yields!(QUEUE.iter(), [Woken::Task(1)]);
    }

    #[test]
    fn duplicate_wakes_queue_the_slot_once() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        assert!(QUEUE.claim().is_some());
        let waker = QUEUE.waker(0);
        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake();
        assert_yields!(QUEUE.iter(), [Woken::Task(0)]);
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
        assert_yields!(QUEUE.iter(), [Woken::Task(0)]);
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
        assert_yields!(QUEUE.iter(), [Woken::Task(1), Woken::Root, Woken::Task(0)]);
    }
}
