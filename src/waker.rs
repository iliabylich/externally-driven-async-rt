use core::{
    ptr::NonNull,
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::{
    ready::{Link, ReadyQueue},
    storage::TaskId,
    thread_id::ThreadId,
};

// The data pointer is the task's `Link` in the ready queue: nothing is owned, so `wake` and
// `wake_by_ref` are the same and dropping is a no-op.
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake, drop_waker);

/// # Safety
///
/// `ready` must stay valid for as long as the waker (or any of its clones) may be woken.
/// Clones may be dropped at any time, from any thread.
pub(crate) unsafe fn new_waker(ready: &ReadyQueue, task: TaskId) -> Waker {
    let link = ready.link(task.slot());
    // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer from
    // `ReadyQueue::link`, which the caller keeps valid while the waker may be woken.
    unsafe { Waker::from_raw(RawWaker::new(link.as_ptr().cast_const().cast(), &VTABLE)) }
}

unsafe fn clone(ptr: *const ()) -> RawWaker {
    RawWaker::new(ptr, &VTABLE)
}

unsafe fn wake(ptr: *const ()) {
    // SAFETY: `ptr` comes from `ReadyQueue::link` in `new_waker`, which is non-null.
    let link = unsafe { NonNull::new_unchecked(ptr.cast::<Link>().cast_mut()) };
    // SAFETY: `new_waker`'s caller guarantees the queue is alive while wakers may be woken.
    if unsafe { ReadyQueue::thread_of(link) } != ThreadId::current() {
        panic!("a Waker must only be woken on the thread of its EventLoop");
    }
    // SAFETY: as above, and we are on the queue's thread (checked right above).
    unsafe { ReadyQueue::push_link(link) };
}

unsafe fn drop_waker(_: *const ()) {}

#[cfg(test)]
mod tests {
    use crate::{
        storage::TaskId,
        test_utils::{TestStorage, assert_yields, stack_memory},
        waker::new_waker,
    };
    use core::{mem::MaybeUninit, task::Waker};

    fn storage_with_task(memory: &mut [MaybeUninit<u8>]) -> (TestStorage<'_>, TaskId) {
        let storage = TestStorage::new(memory, 2, 1, 1);
        let task = storage.storage().tasks.insert(async {});
        (storage, task)
    }

    fn queued<'a>(storage: &'a TestStorage<'_>) -> impl Iterator<Item = TaskId> + 'a {
        core::iter::from_fn(|| storage.ready().pop())
    }

    #[test]
    fn waking_pushes_the_task() {
        let mut memory = stack_memory();
        let (storage, task) = storage_with_task(&mut memory);
        // SAFETY: `storage` outlives the waker.
        let waker = unsafe { new_waker(storage.ready(), task) };
        waker.wake_by_ref();
        assert_yields(queued(&storage), [task]);
        let clone = waker.clone();
        drop(waker);
        clone.wake();
        assert_yields(queued(&storage), [task]);
    }

    #[test]
    fn duplicate_wakes_queue_the_task_once() {
        let mut memory = stack_memory();
        let (storage, task) = storage_with_task(&mut memory);
        // SAFETY: `storage` outlives the waker.
        let waker = unsafe { new_waker(storage.ready(), task) };
        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake();
        assert_yields(queued(&storage), [task]);
    }

    #[test]
    fn waking_after_the_storage_was_mutated() {
        let mut memory = stack_memory();
        let (storage, task) = storage_with_task(&mut memory);
        // SAFETY: `storage` outlives the waker.
        let waker = unsafe { new_waker(storage.ready(), task) };
        let other = storage.storage().tasks.insert(async {});
        storage
            .storage()
            .timers
            .alloc(crate::instant::Instant::now(), Waker::noop().clone());
        waker.wake_by_ref();
        storage.storage().tasks.remove(other);
        waker.wake();
        assert_yields(queued(&storage), [task]);
    }

    #[test]
    fn dropping_does_not_wake() {
        let mut memory = stack_memory();
        let (storage, task) = storage_with_task(&mut memory);
        // SAFETY: `storage` outlives the waker.
        let waker = unsafe { new_waker(storage.ready(), task) };
        drop(waker.clone());
        drop(waker);
        assert!(storage.ready().is_empty());
    }
}
