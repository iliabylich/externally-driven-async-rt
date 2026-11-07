use core::marker::PhantomData;
use core::time::Duration;
use std::time::Instant;

use crate::join_handle::JoinHandle;
use crate::sleep::Sleep;
use crate::state::State;
use crate::storage::Storage;
use crate::task::TaskFuture;

pub struct Context<S: State> {
    storage: Storage,
    _state: PhantomData<S>,
}

impl<S: State> Context<S> {
    // A `Context` only exists inside a task future (see `task_future`) stored in its storage,
    // which drops every task future before freeing its allocation, so `storage` is valid for as
    // long as the `Context` exists.
    pub(crate) fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn dispatch(&self, event: S::Event) {
        // SAFETY: the storage was created with `S`. `dispatch` runs inside a task (polled, or
        // dropped), and tasks only run while the caller of `EventLoop::new`, a drain or `drop`
        // holds exclusive access to the loop: no `state_mut` reference can be live. `apply` gets
        // no `Context` (and `S`, its events and everything they drop are `'static`, so they
        // can't hold one), so it can't re-enter `dispatch`: this is the only reference.
        unsafe { self.storage.state::<S>().as_mut() }.apply(event);
    }

    pub fn spawn<F>(&self, f: F) -> JoinHandle<'_>
    where
        F: AsyncFnOnce(&Context<S>) + 'static,
    {
        // SAFETY: the future is stored in this storage right away.
        let id = self.storage.spawn(unsafe { task_future(self.storage, f) });
        JoinHandle::new(&self.storage, id)
    }

    pub fn sleep_until(&self, deadline: Instant) -> Sleep<'_> {
        Sleep::new(&self.storage, deadline)
    }

    pub fn sleep(&self, duration: Duration) -> Sleep<'_> {
        self.sleep_until(Instant::now() + duration)
    }
}

/// # Safety
///
/// The returned future must be stored in `storage` (and so dropped before the storage is freed),
/// and `storage` must have been created with the state type `S`.
pub(crate) unsafe fn task_future<S, F>(storage: Storage, f: F) -> TaskFuture
where
    S: State,
    F: AsyncFnOnce(&Context<S>) + 'static,
{
    Box::pin(async move {
        let ctx = Context {
            storage,
            _state: PhantomData,
        };
        f(&ctx).await;
    })
}
