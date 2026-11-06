use core::cell::RefCell;
use core::task::{RawWaker, RawWakerVTable, Waker};
use std::thread::{self, ThreadId};

use crate::task::TaskId;

pub(crate) type ReadyQueue = RefCell<Vec<TaskId>>;

#[derive(Clone, Copy)]
struct WakerData {
    ready: *const ReadyQueue,
    task: TaskId,
    thread: ThreadId,
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker);

/// # Safety
///
/// `ready` must stay valid for as long as the waker (or any of its clones) may be woken.
/// Clones may be dropped at any time, from any thread.
pub(crate) unsafe fn new_waker(ready: *const ReadyQueue, task: TaskId) -> Waker {
    let data = WakerData {
        ready,
        task,
        thread: thread::current().id(),
    };
    // SAFETY: the vtable functions below uphold the `RawWaker` contract for a pointer
    // created by `into_raw`.
    unsafe { Waker::from_raw(into_raw(data)) }
}

fn into_raw(data: WakerData) -> RawWaker {
    RawWaker::new(Box::into_raw(Box::new(data)).cast(), &VTABLE)
}

unsafe fn clone(ptr: *const ()) -> RawWaker {
    // SAFETY: `ptr` was created by `into_raw` and is still alive (it's being cloned).
    into_raw(unsafe { *ptr.cast::<WakerData>() })
}

unsafe fn wake(ptr: *const ()) {
    // SAFETY: `ptr` was created by `into_raw`; `wake` consumes it. Owning the box before the
    // thread check frees it even if the check panics.
    let data = unsafe { Box::from_raw(ptr.cast::<WakerData>().cast_mut()) };
    push(*data);
}

unsafe fn wake_by_ref(ptr: *const ()) {
    // SAFETY: `ptr` was created by `into_raw` and is still alive.
    push(unsafe { *ptr.cast::<WakerData>() });
}

fn push(data: WakerData) {
    if data.thread != thread::current().id() {
        panic!("a Waker must only be woken on the thread of its EventLoop");
    }
    // SAFETY: `new_waker`'s caller guarantees `ready` is valid while wakers may be woken, and
    // we are on the loop's thread, so nothing else accesses it concurrently.
    unsafe { (*data.ready).borrow_mut().push(data.task) };
}

unsafe fn drop_waker(ptr: *const ()) {
    // SAFETY: `ptr` was created by `into_raw` and is dropped exactly once. Only the box is
    // freed, the loop is never touched, so this is fine on any thread.
    drop(unsafe { Box::from_raw(ptr.cast::<WakerData>().cast_mut()) });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Tasks;

    fn task_id() -> TaskId {
        Tasks::default().insert(Box::pin(async {}))
    }

    #[test]
    fn waking_pushes_the_task() {
        let ready = ReadyQueue::default();
        let task = task_id();
        // SAFETY: `ready` outlives the waker.
        let waker = unsafe { new_waker(&ready, task) };
        waker.wake_by_ref();
        let clone = waker.clone();
        drop(waker);
        clone.wake();
        assert_eq!(*ready.borrow(), vec![task, task]);
    }

    #[test]
    fn dropping_does_not_wake() {
        let ready = ReadyQueue::default();
        // SAFETY: `ready` outlives the waker.
        let waker = unsafe { new_waker(&ready, task_id()) };
        drop(waker.clone());
        drop(waker);
        assert!(ready.borrow().is_empty());
    }

    #[test]
    fn dropping_on_another_thread_is_fine() {
        let ready = ReadyQueue::default();
        // SAFETY: `ready` outlives the waker.
        let waker = unsafe { new_waker(&ready, task_id()) };
        thread::spawn(move || drop(waker.clone())).join().unwrap();
    }

    #[test]
    fn waking_on_another_thread_panics() {
        let ready = ReadyQueue::default();
        // SAFETY: `ready` outlives the waker.
        let waker = unsafe { new_waker(&ready, task_id()) };
        let result = thread::spawn(move || waker.wake()).join();
        assert!(result.is_err());
        assert!(ready.borrow().is_empty());
    }
}
