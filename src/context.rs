use core::time::Duration;

use crate::{
    instant::Instant, join_handle::JoinHandle, sleep::Sleep, state::State, storage::Storage,
};

pub struct Context<S: State> {
    storage: &'static Storage<S>,
}

impl<S: State> Context<S> {
    // A `Context` only exists inside a task future (see `task_future`) stored in its storage,
    // which drops every task future before freeing its allocation, so `storage` is valid for as
    // long as the `Context` exists.
    pub(crate) fn storage(&self) -> &Storage<S> {
        self.storage
    }

    pub fn dispatch(&self, event: S::Event) {
        // SAFETY: `dispatch` runs inside a task (polled, or dropped), and tasks only run while the
        // caller of `EventLoop::new`, a drain or `drop` holds exclusive access to the loop: no
        // `state_mut` reference can be live. `apply` gets no `Context` (and `S`, its events and
        // everything they drop are `'static`, so they can't hold one), so it can't re-enter
        // `dispatch`: this is the only reference.
        unsafe { &mut *self.storage.state().get() }.apply(event);
    }

    pub fn spawn<F>(&self, f: F) -> JoinHandle<'_, S>
    where
        F: AsyncFnOnce(&Context<S>) + 'static,
    {
        // SAFETY: the future is stored in this storage right away.
        let id = self.storage.spawn(unsafe { task_future(self.storage, f) });
        JoinHandle::new(self.storage, id)
    }

    pub fn sleep(&self, duration: Duration) -> Sleep<'_> {
        Sleep::new(&self.storage.timers, Instant::now() + duration)
    }
}

/// # Safety
///
/// The returned future must be stored in `storage` (and so dropped before the storage is freed).
pub(crate) async unsafe fn task_future<S, F>(storage: &'static Storage<S>, f: F)
where
    S: State,
    F: AsyncFnOnce(&Context<S>) + 'static,
{
    let ctx = Context { storage };
    f(&ctx).await;
}
