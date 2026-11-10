mod link;

use core::{
    sync::atomic::{AtomicU32, AtomicUsize, Ordering::Relaxed},
    task::{RawWaker, RawWakerVTable, Waker},
};

use self::link::Link;
use crate::{sys::ThreadId, tasks::TaskId};

const NONE: u32 = u32::MAX;
const UNTRACKED: u64 = u64::MAX;
const UNCLAIMED: usize = 0;

fn decode(slot: u32) -> Option<u32> {
    (slot != NONE).then_some(slot)
}

pub(crate) struct WakeQueue<const N: usize> {
    owner: AtomicUsize,
    head: AtomicU32,
    tail: AtomicU32,
    links: [Link<N>; N],
}

impl<const N: usize> WakeQueue<N> {
    pub(crate) const fn new() -> Self {
        const {
            assert!(N > 0, "wake queue capacity must not be zero");
            assert!(
                N < NONE as usize,
                "wake queue capacity must be less than u32::MAX"
            );
        }
        let mut links = [const { Link::new(0) }; N];
        let mut slot = 0;
        while slot < N {
            links[slot] = Link::new(slot as u32);
            slot += 1;
        }
        Self {
            owner: AtomicUsize::new(UNCLAIMED),
            head: AtomicU32::new(NONE),
            tail: AtomicU32::new(NONE),
            links,
        }
    }

    pub(crate) fn claim(&self) {
        let claimed =
            self.owner
                .compare_exchange(UNCLAIMED, ThreadId::current().to_raw(), Relaxed, Relaxed);
        assert!(
            claimed.is_ok(),
            "a WakeQueue can only be used by one EventLoop"
        );
    }

    pub(crate) fn waker(&'static self, slot: u32) -> Waker {
        let link = &self.links[slot as usize];
        link.set_queue(self);
        // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer to a
        // link of a queue that is never freed, whose `queue` was just set.
        unsafe {
            Waker::from_raw(RawWaker::new(
                core::ptr::from_ref(link).cast(),
                vtable::<N>(),
            ))
        }
    }

    fn generation(&self, slot: u32) -> Option<u32> {
        let generation = self.links[slot as usize].generation();
        (generation != UNTRACKED).then_some(generation as u32)
    }

    pub(crate) fn track(&self, id: TaskId) {
        let generation = u64::from(id.generation());
        self.links[id.slot() as usize].set_generation(generation);
    }

    pub(crate) fn untrack(&self, id: TaskId) {
        if self.generation(id.slot()) == Some(id.generation()) {
            self.links[id.slot() as usize].set_generation(UNTRACKED);
        }
    }

    pub(crate) fn push(&self, slot: u32) {
        let link = &self.links[slot as usize];
        if self.generation(slot).is_none() || link.is_queued() {
            return;
        }
        link.set_queued(true);
        link.set_next(NONE);
        match decode(self.tail.load(Relaxed)) {
            Some(tail) => self.links[tail as usize].set_next(slot),
            None => self.head.store(slot, Relaxed),
        }
        self.tail.store(slot, Relaxed);
    }

    pub(crate) fn pop(&self) -> Option<TaskId> {
        loop {
            let slot = self.pop_slot()?;
            if let Some(generation) = self.generation(slot) {
                return Some(TaskId::from_parts(slot, generation));
            }
        }
    }

    fn pop_slot(&self) -> Option<u32> {
        let slot = decode(self.head.load(Relaxed))?;
        let link = &self.links[slot as usize];
        let next = link.next();
        self.head.store(next, Relaxed);
        if decode(next).is_none() {
            self.tail.store(NONE, Relaxed);
        }
        link.set_queued(false);
        link.set_next(NONE);
        Some(slot)
    }

    pub(crate) fn is_empty(&self) -> bool {
        decode(self.head.load(Relaxed)).is_none()
    }

    #[cfg(test)]
    pub(crate) fn id_by_waker_data(&'static self, waker: &Waker) -> u32 {
        (0..N as u32)
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
    // SAFETY: `ptr` comes from `WakeQueue::waker`: it points to a link of a queue that is never
    // freed.
    let link = unsafe { &*ptr.cast::<Link<N>>() };
    let queue = link
        .queue()
        .expect("`WakeQueue::waker` sets the queue before creating the waker");
    assert_eq!(
        queue.owner.load(Relaxed),
        ThreadId::current().to_raw(),
        "a Waker must only be woken on the thread of its EventLoop"
    );
    queue.push(link.slot());
}

fn drop_waker(_: *const ()) {}

#[cfg(test)]
mod tests {
    use crate::{
        tasks::{TaskId, WakeQueue},
        testing::assert_yields,
    };

    fn tracked<const N: usize>(queue: &WakeQueue<N>, slot: u32, generation: u32) -> TaskId {
        let id = TaskId::from_parts(slot, generation);
        queue.track(id);
        id
    }

    fn drain<const N: usize>(queue: &WakeQueue<N>) -> impl Iterator<Item = TaskId> + '_ {
        core::iter::from_fn(|| queue.pop())
    }

    #[test]
    fn pops_in_push_order() {
        let queue = WakeQueue::<4>::new();
        let ids = [2, 0, 3].map(|slot| tracked(&queue, slot, 0));
        for id in ids {
            queue.push(id.slot());
        }
        assert!(!queue.is_empty());
        assert_yields!(drain(&queue), ids);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_queued_slot_is_pushed_once() {
        let queue = WakeQueue::<2>::new();
        let a = tracked(&queue, 0, 0);
        let b = tracked(&queue, 1, 0);
        queue.push(0);
        queue.push(1);
        queue.push(0);
        assert_yields!(drain(&queue), [a, b]);
        queue.push(0);
        assert_yields!(drain(&queue), [a]);
    }

    #[test]
    fn re_push_during_draining_goes_to_the_back() {
        let queue = WakeQueue::<2>::new();
        let a = tracked(&queue, 0, 0);
        let b = tracked(&queue, 1, 0);
        queue.push(0);
        queue.push(1);
        assert_eq!(queue.pop(), Some(a));
        queue.push(0);
        assert_yields!(drain(&queue), [b, a]);
    }

    #[test]
    fn pushing_an_untracked_slot_is_ignored() {
        let queue = WakeQueue::<1>::new();
        queue.push(0);
        assert!(queue.is_empty());
        let old = tracked(&queue, 0, 0);
        queue.untrack(old);
        queue.push(0);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_reused_slot_pops_its_new_task() {
        let queue = WakeQueue::<1>::new();
        let old = tracked(&queue, 0, 0);
        queue.untrack(old);
        let new = tracked(&queue, 0, 1);
        queue.push(old.slot());
        assert_yields!(drain(&queue), [new]);
    }

    #[test]
    fn a_task_finished_while_queued_is_skipped() {
        let queue = WakeQueue::<2>::new();
        let a = tracked(&queue, 0, 0);
        let b = tracked(&queue, 1, 0);
        queue.push(a.slot());
        queue.push(b.slot());
        queue.untrack(a);
        assert_yields!(drain(&queue), [b]);
        assert!(queue.is_empty());
    }

    #[test]
    fn untracking_a_newer_task_is_ignored() {
        let queue = WakeQueue::<1>::new();
        let old = TaskId::from_parts(0, 0);
        let new = tracked(&queue, 0, 1);
        queue.untrack(old);
        queue.push(new.slot());
        assert_yields!(drain(&queue), [new]);
    }

    #[test]
    fn waking_pushes_the_slot() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        QUEUE.claim();
        let id = tracked(&QUEUE, 1, 0);
        let waker = QUEUE.waker(1);
        waker.wake_by_ref();
        assert_yields!(drain(&QUEUE), [id]);
        let clone = waker.clone();
        drop(waker);
        clone.wake();
        assert_yields!(drain(&QUEUE), [id]);
    }

    #[test]
    fn duplicate_wakes_queue_the_slot_once() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        QUEUE.claim();
        let id = tracked(&QUEUE, 0, 0);
        let waker = QUEUE.waker(0);
        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake();
        assert_yields!(drain(&QUEUE), [id]);
    }

    #[test]
    fn waking_after_the_queue_was_mutated() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        QUEUE.claim();
        let a = tracked(&QUEUE, 0, 0);
        let b = tracked(&QUEUE, 1, 0);
        let waker = QUEUE.waker(0);
        QUEUE.push(1);
        waker.wake_by_ref();
        assert_eq!(QUEUE.pop(), Some(b));
        waker.wake();
        assert_yields!(drain(&QUEUE), [a]);
    }

    #[test]
    fn dropping_does_not_wake() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        QUEUE.claim();
        tracked(&QUEUE, 0, 0);
        let waker = QUEUE.waker(0);
        drop(waker.clone());
        drop(waker);
        assert!(QUEUE.is_empty());
    }

    #[test]
    #[should_panic(expected = "a WakeQueue can only be used by one EventLoop")]
    fn claiming_twice_panics() {
        let queue = WakeQueue::<1>::new();
        queue.claim();
        queue.claim();
    }
}
