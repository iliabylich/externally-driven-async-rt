use core::time::Duration;
use std::time::Instant;

use crate::event_loop::EventLoop;
use crate::join_handle::JoinHandle;
use crate::sleep::Sleep;
use crate::state::State;
use crate::task::TaskFuture;

pub struct Context<S: State> {
    event_loop: *const EventLoop<S>,
}

impl<S: State> Context<S> {
    pub(crate) fn event_loop(&self) -> &EventLoop<S> {
        // SAFETY: a `Context` only exists inside a task future (see `task_future`) stored in
        // this loop. The loop is pinned, so the pointer stays valid, and its `Drop` drops every
        // task future before any field is freed. `EventLoop` is `!Unpin`, so the `&mut`s to it
        // (`Pin<&mut Self>` methods, `Drop`) don't assert uniqueness over this shared reference.
        unsafe { &*self.event_loop }
    }

    pub fn dispatch(&self, event: S::Event) {
        self.event_loop().dispatch(event);
    }

    pub fn spawn<F>(&self, f: F) -> JoinHandle<'_>
    where
        F: AsyncFnOnce(&Context<S>) + 'static,
    {
        let event_loop = self.event_loop();
        // SAFETY: the future is stored in this loop's task storage right away.
        let id = event_loop.spawn(unsafe { task_future(self.event_loop, f) });
        JoinHandle::new(event_loop.storage(), id)
    }

    pub fn sleep_until(&self, deadline: Instant) -> Sleep<'_> {
        Sleep::new(self.event_loop().storage(), deadline)
    }

    pub fn sleep(&self, duration: Duration) -> Sleep<'_> {
        self.sleep_until(Instant::now() + duration)
    }
}

/// # Safety
///
/// The returned future must be stored in `event_loop`'s task storage (and so dropped before the
/// loop's fields are).
pub(crate) unsafe fn task_future<S, F>(event_loop: *const EventLoop<S>, f: F) -> TaskFuture
where
    S: State,
    F: AsyncFnOnce(&Context<S>) + 'static,
{
    Box::pin(async move {
        let ctx = Context { event_loop };
        f(&ctx).await;
    })
}
