use core::{
    ptr::NonNull,
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::{ready::ReadyQueue, storage::TaskId, thread_id::ThreadId};

fn vtable<const N: usize>() -> &'static RawWakerVTable {
    const { &RawWakerVTable::new(clone::<N>, wake::<N>, wake::<N>, drop_waker) }
}

/// # Safety
///
/// `ready` must stay valid for as long as the waker (or any of its clones) may be woken.
/// Clones may be dropped at any time, from any thread.
pub(crate) unsafe fn new_waker<const N: usize>(ready: &ReadyQueue<N>, task: TaskId) -> Waker {
    let link = ready.link(task.slot());
    // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer from
    // `ReadyQueue::link`, which the caller keeps valid while the waker may be woken.
    unsafe {
        Waker::from_raw(RawWaker::new(
            link.as_ptr().cast_const().cast(),
            vtable::<N>(),
        ))
    }
}

unsafe fn clone<const N: usize>(ptr: *const ()) -> RawWaker {
    RawWaker::new(ptr, vtable::<N>())
}

unsafe fn wake<const N: usize>(ptr: *const ()) {
    // SAFETY: `ptr` comes from `ReadyQueue::link` in `new_waker`, which is non-null.
    let link = unsafe { NonNull::new_unchecked(ptr.cast_mut().cast()) };
    // SAFETY: `new_waker`'s caller guarantees the queue is alive while wakers may be woken, and
    // only `thread` is accessed before it's checked.
    let (queue, slot) = unsafe { ReadyQueue::<N>::of_link(link) };
    assert_eq!(
        queue.initial_thread_id(),
        ThreadId::current(),
        "a Waker must only be woken on the thread of its EventLoop"
    );
    queue.push(slot);
}

unsafe fn drop_waker(_: *const ()) {}

#[cfg(test)]
mod tests {
    use crate::{ready::ReadyQueue, storage::TaskId, test_utils::assert_yields, waker::new_waker};
    use core::mem::MaybeUninit;

    fn queue_with_task(place: &mut MaybeUninit<ReadyQueue<2>>) -> (&ReadyQueue<2>, TaskId) {
        let queue = ReadyQueue::init(place);
        let task = TaskId::from_parts(0, 0);
        queue.track(task);
        (queue, task)
    }

    fn queued(queue: &ReadyQueue<2>) -> impl Iterator<Item = TaskId> + '_ {
        core::iter::from_fn(|| queue.pop())
    }

    #[test]
    fn waking_pushes_the_task() {
        let mut place = MaybeUninit::uninit();
        let (queue, task) = queue_with_task(&mut place);
        // SAFETY: `queue` outlives the waker.
        let waker = unsafe { new_waker(queue, task) };
        waker.wake_by_ref();
        assert_yields(queued(queue), [task]);
        let clone = waker.clone();
        drop(waker);
        clone.wake();
        assert_yields(queued(queue), [task]);
    }

    #[test]
    fn duplicate_wakes_queue_the_task_once() {
        let mut place = MaybeUninit::uninit();
        let (queue, task) = queue_with_task(&mut place);
        // SAFETY: `queue` outlives the waker.
        let waker = unsafe { new_waker(queue, task) };
        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake();
        assert_yields(queued(queue), [task]);
    }

    #[test]
    fn waking_after_the_queue_was_mutated() {
        let mut place = MaybeUninit::uninit();
        let (queue, task) = queue_with_task(&mut place);
        // SAFETY: `queue` outlives the waker.
        let waker = unsafe { new_waker(queue, task) };
        let other = TaskId::from_parts(1, 0);
        queue.track(other);
        queue.push(other.slot());
        waker.wake_by_ref();
        queue.untrack(other);
        waker.wake();
        assert_yields(queued(queue), [task]);
    }

    #[test]
    fn dropping_does_not_wake() {
        let mut place = MaybeUninit::uninit();
        let (queue, task) = queue_with_task(&mut place);
        // SAFETY: `queue` outlives the waker.
        let waker = unsafe { new_waker(queue, task) };
        drop(waker.clone());
        drop(waker);
        assert!(queue.is_empty());
    }
}
