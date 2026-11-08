use core::time::Duration;

use crate::{
    instant::Instant, join_handle::JoinHandle, sleep::Sleep, state::State, storage::Storage,
};

pub struct Context<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> {
    storage: &'static Storage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
}

impl<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Context<S, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE, MAX_TIMERS_COUNT, MAX_IO_COUNT>
{
    pub(crate) fn storage(
        &self,
    ) -> &Storage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    > {
        self.storage
    }

    pub fn dispatch(&self, event: S::Event) {
        // SAFETY: `dispatch` runs inside a task (polled, or dropped), and tasks only run while the
        // caller of `EventLoop::new`, a drain or `drop` holds exclusive access to the loop: no
        // `state_mut` reference can be live. `apply` gets no `Context` (and `S`, its events and
        // everything they drop are `'static`, so they can't hold one), so it can't re-enter
        // `dispatch`: this is the only reference.
        unsafe { &mut *self.storage.state.get() }.apply(event);
    }

    pub fn spawn<F>(
        &self,
        f: F,
    ) -> JoinHandle<'_, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
    where
        F: AsyncFnOnce(&Self) + 'static,
    {
        // SAFETY: the future is stored in this storage right away.
        let id = self
            .storage
            .tasks
            .spawn(unsafe { task_future(self.storage, f) });
        JoinHandle::new(&self.storage.tasks, id)
    }

    pub fn sleep(&self, duration: Duration) -> Sleep<'_, MAX_TIMERS_COUNT> {
        Sleep::new(&self.storage.timers, Instant::now() + duration)
    }
}

/// # Safety
///
/// The returned future must be stored in `storage` (and so dropped before the storage is
/// destroyed).
pub(crate) async unsafe fn task_future<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    F,
>(
    storage: &'static Storage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    f: F,
) where
    F: AsyncFnOnce(
            &Context<
                S,
                MAX_TASKS_COUNT,
                MAX_TASK_SIZE,
                MAX_ROOT_TASK_SIZE,
                MAX_TIMERS_COUNT,
                MAX_IO_COUNT,
            >,
        ) + 'static,
{
    let ctx = Context { storage };
    f(&ctx).await;
}
