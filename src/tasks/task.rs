use core::{
    future::Future,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

pub(crate) struct Task(NonNull<dyn Future<Output = ()>>);

impl Task {
    /// # Safety
    ///
    /// `memory` must be valid for writes of `Fut`, aligned for it, and stay valid and otherwise
    /// unused until the returned `Task` is dropped. The returned `Task` must be polled and dropped
    /// (if at all) only while everything `Fut` borrows is still alive.
    pub(crate) unsafe fn write<Fut>(memory: NonNull<u8>, future: Fut) -> Self
    where
        Fut: Future<Output = ()>,
    {
        let memory = memory.cast::<Fut>();
        let task: NonNull<dyn Future<Output = ()> + '_> = memory;
        // SAFETY: guaranteed by the caller. Erasing the trait object's lifetime (the two pointer
        // types differ only in it, so have the same layout) is fine because the future is only
        // polled and dropped through the `Task`, which the caller drops while everything `Fut`
        // borrows is still alive.
        unsafe {
            memory.write(future);
            Self(core::mem::transmute::<
                NonNull<dyn Future<Output = ()> + '_>,
                NonNull<dyn Future<Output = ()>>,
            >(task))
        }
    }

    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the future was written by `write` and stays valid until `self` is dropped
        // (guaranteed by `write`'s caller, as is everything it borrows being alive). Only `self`
        // moves, never the future, so pinning holds, and `&mut self` rules out any other access.
        unsafe { Pin::new_unchecked(self.0.as_mut()) }.poll(cx)
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        // SAFETY: the future was written by `write`, its memory is still valid (guaranteed by
        // `write`'s caller), and `drop` runs at most once.
        unsafe { core::ptr::drop_in_place(self.0.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use core::{
        mem::MaybeUninit,
        ptr::NonNull,
        sync::atomic::{AtomicBool, Ordering::Relaxed},
        task::{Context, Poll, Waker},
    };

    use crate::{tasks::Task, testing::FlagOnDrop};

    fn write_task<Fut>(memory: &mut MaybeUninit<Fut>, future: Fut) -> Task
    where
        Fut: Future<Output = ()>,
    {
        // SAFETY: `memory` fits `Fut`, and it and everything `future` borrows outlive the returned
        // task in every test.
        unsafe { Task::write(NonNull::from(memory).cast(), future) }
    }

    #[test]
    fn polls_in_place_while_the_owner_moves() {
        let mut polled_once = false;
        let mut memory = MaybeUninit::uninit();
        let mut task = write_task(
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
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(task.poll(&mut cx), Poll::Pending);
        let mut moved = [task];
        assert_eq!(moved[0].poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn dropping_drops_the_future_in_place() {
        let dropped = AtomicBool::new(false);
        let mut memory = MaybeUninit::uninit();
        let task = write_task(&mut memory, FlagOnDrop::new(&dropped));
        drop(task);
        assert!(dropped.load(Relaxed));
    }
}
