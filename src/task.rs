use core::future::Future;
use core::pin::Pin;
use core::ptr::{self, NonNull};
use core::task::{Context, Poll};

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
    /// The `OwnedTask` this pointer came from must not be dropped while this runs, and the same
    /// task must not be polled re-entrantly.
    pub(crate) unsafe fn poll(self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the pointer comes from `OwnedTask::write` and stays valid until the
        // `OwnedTask` is dropped, which the caller rules out for the duration of the poll; the
        // future is never moved, so pinning holds; no other reference to it exists because it
        // is only ever accessed here, never re-entrantly.
        unsafe { Pin::new_unchecked(&mut *self.0.as_ptr()).poll(cx) }
    }
}

// Owns a task future written into memory it doesn't own (its slot of the task slab), at a fixed
// address, and drops it in place. A raw pointer, not a reference: the storage moves the owner
// around while `TaskPtr`s poll the future in place.
pub(crate) struct OwnedTask(TaskPtr);

impl OwnedTask {
    /// # Safety
    ///
    /// `memory` must be valid for writes of `Fut`, aligned for it, and stay valid and otherwise
    /// unused until the returned `OwnedTask` is dropped.
    pub(crate) unsafe fn write<Fut>(memory: NonNull<u8>, future: Fut) -> Self
    where
        Fut: Future<Output = ()> + 'static,
    {
        let memory = memory.cast::<Fut>();
        // SAFETY: guaranteed by the caller.
        unsafe { memory.write(future) };
        Self(TaskPtr(memory))
    }

    pub(crate) fn ptr(&self) -> TaskPtr {
        self.0
    }
}

impl Drop for OwnedTask {
    fn drop(&mut self) {
        // SAFETY: the future was written by `write`, its memory is still valid (guaranteed by
        // `write`'s caller), and `drop` runs at most once.
        unsafe { ptr::drop_in_place(self.0.0.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::MaybeUninit;
    use core::task::Waker;

    struct DropFlag(*mut bool);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            // SAFETY: points to a `static mut` of the running test, which only that test's thread
            // uses, and no reference to it exists.
            unsafe { *self.0 = true };
        }
    }

    fn owned<Fut>(memory: &mut MaybeUninit<Fut>, future: Fut) -> OwnedTask
    where
        Fut: Future<Output = ()> + 'static,
    {
        // SAFETY: `memory` fits `Fut` and outlives the returned task in every test.
        unsafe { OwnedTask::write(NonNull::from(memory).cast(), future) }
    }

    #[test]
    fn polls_in_place_while_the_owner_moves() {
        let mut polled_once = false;
        let mut memory = MaybeUninit::uninit();
        let task = owned(
            &mut memory,
            core::future::poll_fn(move |_| {
                if polled_once {
                    Poll::Ready(())
                } else {
                    polled_once = true;
                    Poll::Pending
                }
            }),
        );
        let ptr = task.ptr();
        let mut cx = Context::from_waker(Waker::noop());
        // SAFETY: `task` is alive and not polled re-entrantly.
        assert_eq!(unsafe { ptr.poll(&mut cx) }, Poll::Pending);
        let moved = [task];
        // SAFETY: as above, the owner moved but the future didn't.
        assert_eq!(unsafe { moved[0].ptr().poll(&mut cx) }, Poll::Ready(()));
    }

    #[test]
    fn dropping_drops_the_future_in_place() {
        static mut DROPPED: bool = false;
        let flag = DropFlag(&raw mut DROPPED);
        let mut memory = MaybeUninit::uninit();
        let task = owned(&mut memory, async move {
            let _flag = flag;
        });
        drop(task);
        // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
        assert!(unsafe { DROPPED });
    }
}
