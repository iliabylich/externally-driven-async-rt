use core::future::Future;
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::{Context, Poll};

pub(crate) type TaskFuture = Pin<Box<dyn Future<Output = ()>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinStatus {
    Running,
    Finished,
}

#[derive(Clone, Copy)]
pub(crate) struct TaskPtr(NonNull<dyn Future<Output = ()>>);

impl TaskPtr {
    /// # Safety
    ///
    /// The `OwnedTask` this pointer came from must not be dropped or turned back into a future
    /// while this runs, and the same task must not be polled re-entrantly.
    pub(crate) unsafe fn poll(self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the pointer comes from `Box::leak` in `OwnedTask::new` and stays valid until
        // the `OwnedTask` frees it, which the caller rules out for the duration of the poll; the
        // future is never moved, so pinning holds; no other reference to it exists because it
        // is only ever accessed here, never re-entrantly.
        unsafe { Pin::new_unchecked(&mut *self.0.as_ptr()).poll(cx) }
    }
}

// Owns a task future at a fixed heap address (leaked, not a `Box`: the storage moves its owner
// around, and moving a `Box` would assert uniqueness over the `TaskPtr`s polling it in place).
pub(crate) struct OwnedTask(TaskPtr);

impl OwnedTask {
    pub(crate) fn new(future: TaskFuture) -> Self {
        // SAFETY: the future is never moved out of its allocation; it is only polled in place
        // through `TaskPtr` and finally turned back into a `Box` by `into_future` or `drop`.
        let future = Box::leak(unsafe { Pin::into_inner_unchecked(future) });
        Self(TaskPtr(NonNull::from(future)))
    }

    pub(crate) fn ptr(&self) -> TaskPtr {
        self.0
    }

    pub(crate) fn into_future(self) -> TaskFuture {
        let this = ManuallyDrop::new(self);
        // SAFETY: the pointer comes from `Box::leak` in `new`, and `this` is never dropped, so
        // this is the only place that takes ownership of the allocation.
        unsafe { into_box(this.0) }
    }
}

impl Drop for OwnedTask {
    fn drop(&mut self) {
        // SAFETY: as in `into_future`; `drop` runs at most once and never after `into_future`.
        drop(unsafe { into_box(self.0) });
    }
}

/// # Safety
///
/// `task` must come from `OwnedTask::new` and its allocation must not be owned by anyone else.
unsafe fn into_box(task: TaskPtr) -> TaskFuture {
    // SAFETY: guaranteed by the caller.
    unsafe { Box::into_pin(Box::from_raw(task.0.as_ptr())) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;
    use core::task::Waker;
    use std::rc::Rc;

    struct DropFlag(Rc<Cell<bool>>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    #[test]
    fn polls_in_place_while_the_owner_moves() {
        let mut polled_once = false;
        let task = OwnedTask::new(Box::pin(core::future::poll_fn(move |_| {
            if polled_once {
                Poll::Ready(())
            } else {
                polled_once = true;
                Poll::Pending
            }
        })));
        let ptr = task.ptr();
        let mut cx = Context::from_waker(Waker::noop());
        // SAFETY: `task` is alive and not polled re-entrantly.
        assert_eq!(unsafe { ptr.poll(&mut cx) }, Poll::Pending);
        let moved = [task];
        // SAFETY: as above, the owner moved but the future didn't.
        assert_eq!(unsafe { moved[0].ptr().poll(&mut cx) }, Poll::Ready(()));
    }

    #[test]
    fn into_future_hands_back_the_future() {
        let dropped = Rc::new(Cell::new(false));
        let flag = DropFlag(dropped.clone());
        let task = OwnedTask::new(Box::pin(async move {
            let _flag = flag;
        }));
        let future = task.into_future();
        assert!(!dropped.get());
        drop(future);
        assert!(dropped.get());
    }

    #[test]
    fn dropping_frees_the_future() {
        let dropped = Rc::new(Cell::new(false));
        let flag = DropFlag(dropped.clone());
        let task = OwnedTask::new(Box::pin(async move {
            let _flag = flag;
        }));
        drop(task);
        assert!(dropped.get());
    }
}
