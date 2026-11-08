use core::{
    cell::Cell,
    mem::{MaybeUninit, offset_of},
    ptr::NonNull,
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::thread_id::ThreadId;

pub(crate) struct Link<T> {
    slot: u32,
    queued: Cell<bool>,
    next: Cell<Option<u32>>,
    value: T,
}

pub(crate) struct CurrentThreadQueue<T: Default, const N: usize> {
    thread: ThreadId,
    head: Cell<Option<u32>>,
    tail: Cell<Option<u32>>,
    links: [Link<T>; N],
}

impl<T: Default, const N: usize> CurrentThreadQueue<T, N> {
    pub(crate) fn init(place: &mut MaybeUninit<Self>) -> &Self {
        const {
            assert!(N > 0, "current thread queue capacity must not be zero");
            assert!(
                N <= u32::MAX as usize,
                "current thread queue capacity must fit in u32"
            );
        }

        let ptr = place.as_mut_ptr();
        // SAFETY: `ptr` points to `place`, borrowed exclusively until the end; the fields are
        // disjoint, `&raw mut` creates no reference to them while they're uninitialized, and
        // `MaybeUninit<T>` has `T`'s layout.
        let (thread, head, tail, links) = unsafe {
            (
                &mut *(&raw mut (*ptr).thread).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).head).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).tail).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).links).cast::<[MaybeUninit<_>; N]>(),
            )
        };
        thread.write(ThreadId::current());
        head.write(Cell::new(None::<u32>));
        tail.write(Cell::new(None::<u32>));
        for (slot, link) in links.iter_mut().enumerate() {
            link.write(Link {
                slot: slot as u32,
                queued: Cell::new(false),
                next: Cell::new(None),
                value: T::default(),
            });
        }
        // SAFETY: every field was written above.
        unsafe { place.assume_init_ref() }
    }

    pub(crate) fn link(&self, slot: u32) -> NonNull<Link<T>> {
        assert!((slot as usize) < N, "slot {slot} is out of bounds");
        // SAFETY: `links[slot]` is inside `self`; the pointer is derived from all of `self` (not
        // just the link) so that `wake` may step back to the queue from it.
        unsafe {
            NonNull::from(self)
                .byte_add(offset_of!(Self, links))
                .cast::<Link<T>>()
                .add(slot as usize)
        }
    }

    pub(crate) fn waker(&'static self, slot: u32) -> Waker
    where
        T: 'static,
    {
        let link = self.link(slot);
        // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer from
        // `Self::link` of a queue that is never freed.
        unsafe {
            Waker::from_raw(RawWaker::new(
                link.as_ptr().cast_const().cast(),
                vtable::<T, N>(),
            ))
        }
    }

    pub(crate) fn get(&self, slot: u32) -> &T {
        &self.links[slot as usize].value
    }

    pub(crate) fn push(&self, slot: u32) {
        let link = &self.links[slot as usize];
        if link.queued.get() {
            return;
        }
        link.queued.set(true);
        link.next.set(None);
        match self.tail.get() {
            Some(tail) => self.links[tail as usize].next.set(Some(slot)),
            None => self.head.set(Some(slot)),
        }
        self.tail.set(Some(slot));
    }

    pub(crate) fn pop(&self) -> Option<u32> {
        let slot = self.head.get()?;
        let link = &self.links[slot as usize];
        self.head.set(link.next.get());
        if self.head.get().is_none() {
            self.tail.set(None);
        }
        link.queued.set(false);
        link.next.set(None);
        Some(slot)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.head.get().is_none()
    }
}

fn vtable<T: Default, const N: usize>() -> &'static RawWakerVTable {
    const { &RawWakerVTable::new(clone::<T, N>, wake::<T, N>, wake::<T, N>, drop_waker) }
}

fn clone<T: Default, const N: usize>(ptr: *const ()) -> RawWaker {
    RawWaker::new(ptr, vtable::<T, N>())
}

unsafe fn wake<T: Default, const N: usize>(ptr: *const ()) {
    let link = ptr.cast::<Link<T>>();
    // SAFETY: `ptr` comes from `CurrentThreadQueue::link` in `CurrentThreadQueue::waker`, of a
    // queue that is never freed. `slot` isn't written after `init`,
    // and it's read through a raw pointer, so no reference to the link's `Cell`s is created.
    let slot = unsafe { (*link).slot };
    // SAFETY: `link` points to `links[slot]` of the queue and carries the provenance of the whole
    // queue (see `CurrentThreadQueue::link`), so stepping back by its offset stays inside the live
    // queue. After `init` the queue is only ever accessed through shared references, so creating
    // one reads only bytes outside `UnsafeCell`s, which are never written again, and doesn't race
    // with the queue's thread; nothing else is accessed before `thread` is checked below.
    let queue = unsafe {
        &*link
            .byte_sub(
                offset_of!(CurrentThreadQueue<T, N>, links) + slot as usize * size_of::<Link<T>>(),
            )
            .cast::<CurrentThreadQueue<T, N>>()
    };
    assert_eq!(
        queue.thread,
        ThreadId::current(),
        "a Waker must only be woken on the thread of its EventLoop"
    );
    queue.push(slot);
}

fn drop_waker(_: *const ()) {}

#[cfg(test)]
mod tests {
    use crate::{
        current_thread_queue::CurrentThreadQueue,
        test_utils::{assert_yields, static_within_current_test},
    };
    use core::{cell::Cell, mem::MaybeUninit};

    fn drain<T: Default, const N: usize>(
        queue: &CurrentThreadQueue<T, N>,
    ) -> impl Iterator<Item = u32> + '_ {
        core::iter::from_fn(|| queue.pop())
    }

    #[test]
    fn pops_in_push_order() {
        let mut place = MaybeUninit::uninit();
        let queue = CurrentThreadQueue::<(), 4>::init(&mut place);
        for slot in [2, 0, 3] {
            queue.push(slot);
        }
        assert!(!queue.is_empty());
        assert_yields(drain(queue), [2, 0, 3]);
        assert!(queue.is_empty());
    }

    #[test]
    fn a_queued_slot_is_pushed_once() {
        let mut place = MaybeUninit::uninit();
        let queue = CurrentThreadQueue::<(), 2>::init(&mut place);
        queue.push(0);
        queue.push(1);
        queue.push(0);
        assert_yields(drain(queue), [0, 1]);
        queue.push(0);
        assert_yields(drain(queue), [0]);
    }

    #[test]
    fn re_push_during_draining_goes_to_the_back() {
        let mut place = MaybeUninit::uninit();
        let queue = CurrentThreadQueue::<(), 2>::init(&mut place);
        queue.push(0);
        queue.push(1);
        assert_eq!(queue.pop(), Some(0));
        queue.push(0);
        assert_yields(drain(queue), [1, 0]);
    }

    #[test]
    fn values_are_per_slot() {
        let mut place = MaybeUninit::uninit();
        let queue = CurrentThreadQueue::<Cell<u32>, 2>::init(&mut place);
        queue.get(1).set(7);
        assert_eq!(queue.get(0).get(), 0);
        assert_eq!(queue.get(1).get(), 7);
    }

    #[test]
    fn waking_pushes_the_slot() {
        let queue =
            CurrentThreadQueue::init(static_within_current_test!(CurrentThreadQueue<(), 2>));
        let waker = queue.waker(1);
        waker.wake_by_ref();
        assert_yields(drain(queue), [1]);
        let clone = waker.clone();
        drop(waker);
        clone.wake();
        assert_yields(drain(queue), [1]);
    }

    #[test]
    fn duplicate_wakes_queue_the_slot_once() {
        let queue =
            CurrentThreadQueue::init(static_within_current_test!(CurrentThreadQueue<(), 2>));
        let waker = queue.waker(0);
        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake();
        assert_yields(drain(queue), [0]);
    }

    #[test]
    fn waking_after_the_queue_was_mutated() {
        let queue =
            CurrentThreadQueue::init(static_within_current_test!(CurrentThreadQueue<(), 2>));
        let waker = queue.waker(0);
        queue.push(1);
        waker.wake_by_ref();
        assert_eq!(queue.pop(), Some(1));
        waker.wake();
        assert_yields(drain(queue), [0]);
    }

    #[test]
    fn dropping_does_not_wake() {
        let queue =
            CurrentThreadQueue::init(static_within_current_test!(CurrentThreadQueue<(), 2>));
        let waker = queue.waker(0);
        drop(waker.clone());
        drop(waker);
        assert!(queue.is_empty());
    }

    #[test]
    #[should_panic(expected = "slot 2 is out of bounds")]
    fn link_out_of_bounds_panics() {
        let mut place = MaybeUninit::uninit();
        let queue = CurrentThreadQueue::<(), 2>::init(&mut place);
        queue.link(2);
    }
}
