use core::{
    mem::offset_of,
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed},
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::{storage::TaskId, thread_id::ThreadId};

const NONE: u32 = u32::MAX;
const UNTRACKED: u64 = u64::MAX;
const UNCLAIMED: usize = 0;

fn decode(slot: u32) -> Option<u32> {
    (slot != NONE).then_some(slot)
}

struct Link {
    slot: u32,
    queued: AtomicBool,
    next: AtomicU32,
    generation: AtomicU64,
}

pub(crate) struct WakeQueue<const N: usize> {
    owner: AtomicUsize,
    head: AtomicU32,
    tail: AtomicU32,
    links: [Link; N],
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
        let mut links = [const {
            Link {
                slot: 0,
                queued: AtomicBool::new(false),
                next: AtomicU32::new(NONE),
                generation: AtomicU64::new(UNTRACKED),
            }
        }; N];
        let mut slot = 0;
        while slot < N {
            links[slot].slot = slot as u32;
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

    fn link(&self, slot: u32) -> NonNull<Link> {
        assert!((slot as usize) < N, "slot {slot} is out of bounds");
        // SAFETY: `links[slot]` is inside `self`; the pointer is derived from all of `self` (not
        // just the link) so that `wake` may step back to the queue from it.
        unsafe {
            NonNull::from(self)
                .byte_add(offset_of!(Self, links))
                .cast::<Link>()
                .add(slot as usize)
        }
    }

    pub(crate) fn waker(&'static self, slot: u32) -> Waker {
        let link = self.link(slot);
        // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer from
        // `Self::link` of a queue that is never freed.
        unsafe {
            Waker::from_raw(RawWaker::new(
                link.as_ptr().cast_const().cast(),
                vtable::<N>(),
            ))
        }
    }

    fn generation(&self, slot: u32) -> Option<u32> {
        let generation = self.links[slot as usize].generation.load(Relaxed);
        (generation != UNTRACKED).then_some(generation as u32)
    }

    pub(crate) fn track(&self, id: TaskId) {
        let generation = u64::from(id.generation());
        self.links[id.slot() as usize]
            .generation
            .store(generation, Relaxed);
    }

    pub(crate) fn untrack(&self, id: TaskId) {
        if self.generation(id.slot()) == Some(id.generation()) {
            self.links[id.slot() as usize]
                .generation
                .store(UNTRACKED, Relaxed);
        }
    }

    pub(crate) fn push(&self, slot: u32) {
        let link = &self.links[slot as usize];
        if self.generation(slot).is_none() || link.queued.load(Relaxed) {
            return;
        }
        link.queued.store(true, Relaxed);
        link.next.store(NONE, Relaxed);
        match decode(self.tail.load(Relaxed)) {
            Some(tail) => self.links[tail as usize].next.store(slot, Relaxed),
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
        let next = link.next.load(Relaxed);
        self.head.store(next, Relaxed);
        if decode(next).is_none() {
            self.tail.store(NONE, Relaxed);
        }
        link.queued.store(false, Relaxed);
        link.next.store(NONE, Relaxed);
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
    let link = ptr.cast::<Link>();
    // SAFETY: `ptr` comes from `WakeQueue::link` in `WakeQueue::waker`, of a queue that is never
    // freed. `slot` isn't written after `new`.
    let slot = unsafe { (*link).slot };
    // SAFETY: `link` points to `links[slot]` of the queue and carries the provenance of the whole
    // queue (see `WakeQueue::link`), so stepping back by its offset stays inside the live queue.
    let queue = unsafe {
        &*link
            .byte_sub(offset_of!(WakeQueue<N>, links) + slot as usize * size_of::<Link>())
            .cast::<WakeQueue<N>>()
    };
    assert_eq!(
        queue.owner.load(Relaxed),
        ThreadId::current().to_raw(),
        "a Waker must only be woken on the thread of its EventLoop"
    );
    queue.push(slot);
}

fn drop_waker(_: *const ()) {}

#[cfg(test)]
mod tests {
    use crate::{storage::TaskId, test_utils::assert_yields, wake_queue::WakeQueue};

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
        assert_yields(drain(&queue), ids);
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
        assert_yields(drain(&queue), [a, b]);
        queue.push(0);
        assert_yields(drain(&queue), [a]);
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
        assert_yields(drain(&queue), [b, a]);
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
        assert_yields(drain(&queue), [new]);
    }

    #[test]
    fn a_task_finished_while_queued_is_skipped() {
        let queue = WakeQueue::<2>::new();
        let a = tracked(&queue, 0, 0);
        let b = tracked(&queue, 1, 0);
        queue.push(a.slot());
        queue.push(b.slot());
        queue.untrack(a);
        assert_yields(drain(&queue), [b]);
        assert!(queue.is_empty());
    }

    #[test]
    fn untracking_a_newer_task_is_ignored() {
        let queue = WakeQueue::<1>::new();
        let old = TaskId::from_parts(0, 0);
        let new = tracked(&queue, 0, 1);
        queue.untrack(old);
        queue.push(new.slot());
        assert_yields(drain(&queue), [new]);
    }

    #[test]
    fn waking_pushes_the_slot() {
        static QUEUE: WakeQueue<2> = WakeQueue::new();
        QUEUE.claim();
        let id = tracked(&QUEUE, 1, 0);
        let waker = QUEUE.waker(1);
        waker.wake_by_ref();
        assert_yields(drain(&QUEUE), [id]);
        let clone = waker.clone();
        drop(waker);
        clone.wake();
        assert_yields(drain(&QUEUE), [id]);
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
        assert_yields(drain(&QUEUE), [id]);
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
        assert_yields(drain(&QUEUE), [a]);
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

    #[test]
    #[should_panic(expected = "slot 2 is out of bounds")]
    fn link_out_of_bounds_panics() {
        let queue = WakeQueue::<2>::new();
        queue.link(2);
    }
}
