use core::{future::Future, time::Duration};

use crate::{
    state::State,
    storage::LoopStorageGuard,
    tasks::{JoinHandle, ReserveTask},
    time::{Instant, Sleep, Timeout},
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
    storage: LoopStorageGuard<
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
    pub(crate) fn new(
        storage: LoopStorageGuard<
            'a,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
    ) -> Self {
        Self { storage }
    }

    pub(crate) fn storage(
        &self,
    ) -> &LoopStorageGuard<
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
        self.storage
            .state_slot()
            .borrow_mut()
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
            .spawn(async move { f(&Self::new(storage)).await });
        JoinHandle::new(self.storage.tasks, id)
    }

    pub fn reserve_task(
        &self,
    ) -> ReserveTask<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    > {
        ReserveTask::new(self.storage)
    }

    pub(crate) fn spawn_root<F>(
        storage: LoopStorageGuard<
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
            .spawn_root(async move { f(&Self::new(storage)).await });
    }

    pub fn sleep(&self, duration: Duration) -> Sleep<'_, MAX_TIMERS_COUNT> {
        Sleep::new(self.storage.timers(), Instant::now() + duration)
    }

    pub fn timeout<F: Future>(
        &self,
        duration: Duration,
        future: F,
    ) -> Timeout<'_, F, MAX_TIMERS_COUNT> {
        Timeout::new(future, self.sleep(duration))
    }
}
