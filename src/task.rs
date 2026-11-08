use core::{
    future::Future,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

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
    /// The `OwnedTask` this pointer came from must not be dropped while this runs, the lifetime
    /// it was written for must not have ended, and the same task must not be polled re-entrantly.
    pub(crate) unsafe fn poll(self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the pointer comes from `OwnedTask::write` and stays valid until the
        // `OwnedTask` is dropped, which the caller rules out for the duration of the poll; the
        // future is never moved, so pinning holds; no other reference to it exists because it
        // is only ever accessed here, never re-entrantly.
        unsafe { Pin::new_unchecked(&mut *self.0.as_ptr()).poll(cx) }
    }
}

pub(crate) struct OwnedTask(TaskPtr);

impl OwnedTask {
    /// # Safety
    ///
    /// `memory` must be valid for writes of `Fut`, aligned for it, and stay valid and otherwise
    /// unused until the returned `OwnedTask` is dropped. The returned `OwnedTask` must be dropped
    /// (if at all) before `'a` ends.
    pub(crate) unsafe fn write<'a, Fut>(memory: NonNull<u8>, future: Fut) -> Self
    where
        Fut: Future<Output = ()> + 'a,
    {
        let memory = memory.cast::<Fut>();
        let task: NonNull<dyn Future<Output = ()> + 'a> = memory;
        // SAFETY: guaranteed by the caller. Erasing `'a` (the two pointer types differ only in it,
        // so have the same layout) is fine because the future is only polled while `'a` lasts
        // (see `TaskPtr::poll`) and dropped before it ends.
        unsafe {
            memory.write(future);
            Self(TaskPtr(core::mem::transmute::<
                NonNull<dyn Future<Output = ()> + 'a>,
                NonNull<dyn Future<Output = ()>>,
            >(task)))
        }
    }

    pub(crate) fn ptr(&self) -> TaskPtr {
        self.0
    }
}

impl Drop for OwnedTask {
    fn drop(&mut self) {
        // SAFETY: the future was written by `write`, its memory is still valid (guaranteed by
        // `write`'s caller), and `drop` runs at most once.
        unsafe { core::ptr::drop_in_place(self.0.0.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use core::{
        cell::Cell,
        mem::MaybeUninit,
        ptr::NonNull,
        task::{Context, Poll, Waker},
    };

    use crate::{task::OwnedTask, test_utils::flagged_task};

    fn owned<Fut>(memory: &mut MaybeUninit<Fut>, future: Fut) -> OwnedTask
    where
        Fut: Future<Output = ()>,
    {
        // SAFETY: `memory` fits `Fut`, and it and everything `future` borrows outlive the returned
        // task in every test.
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
        let dropped = Cell::new(false);
        let mut memory = MaybeUninit::uninit();
        let task = owned(&mut memory, flagged_task(&dropped));
        drop(task);
        assert!(dropped.get());
    }
}
