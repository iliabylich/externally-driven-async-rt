use core::time::Duration;

use crate::{
    instant::Instant, join_handle::JoinHandle, sleep::Sleep, state::State, storage::StorageGuard,
};

pub struct Context<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> {
    storage: StorageGuard<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
}

impl<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>
    Context<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    pub(crate) fn storage(
        &self,
    ) -> &StorageGuard<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    > {
        &self.storage
    }

    pub fn dispatch(&self, event: S::Event) {
        // SAFETY: a `Context` is only created inside the futures that `spawn` and `spawn_root`
        // store as tasks right away, so `dispatch` runs inside a task (polled, or dropped), and
        // tasks only run while the caller of `EventLoop::new`, a drain or `drop` holds exclusive
        // access to the loop: no `state_mut` reference can be live. `apply` gets no `Context`, and
        // can't reach one: the task lends it to its closure for a higher-ranked lifetime, so it
        // can't be stored in `S` or its events, whose lifetimes are fixed when the loop is
        // created. So `apply` can't re-enter `dispatch`: this is the only reference.
        unsafe { &mut *self.storage.state.get() }
            .as_mut()
            .expect("the state is set while tasks run")
            .apply(event);
    }

    pub fn spawn<F>(
        &self,
        f: F,
    ) -> JoinHandle<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
    where
        F: AsyncFnOnce(&Self) + 'a,
    {
        let storage = self.storage;
        let id = storage
            .tasks
            .spawn(async move { f(&Self { storage }).await });
        JoinHandle::new(self.storage.tasks, id)
    }

    pub(crate) fn spawn_root<F>(
        storage: StorageGuard<
            'a,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        f: F,
    ) where
        F: AsyncFnOnce(&Self) + 'a,
    {
        storage
            .tasks
            .spawn_root(async move { f(&Self { storage }).await });
    }

    pub fn sleep(&self, duration: Duration) -> Sleep<'_, MAX_TIMERS_COUNT> {
        Sleep::new(self.storage.timers, Instant::now() + duration)
    }
}
