use crate::{
    context::Context,
    state::State,
    storage::LoopStorageGuard,
    tasks::{JoinHandle, TaskId},
};

#[must_use = "a reservation holds a task slot until it is spawned into or dropped"]
pub struct TaskReservation<
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
    id: TaskId,
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
    TaskReservation<
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
        id: TaskId,
    ) -> Self {
        Self { storage, id }
    }

    pub fn spawn<F, Fut>(
        self,
        f: F,
    ) -> JoinHandle<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
    where
        F: FnOnce(
            Context<
                'a,
                S,
                MAX_TASKS_COUNT,
                MAX_TASK_SIZE,
                MAX_ROOT_TASK_SIZE,
                MAX_TIMERS_COUNT,
                MAX_IO_COUNT,
            >,
        ) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let Self { storage, id } = self;
        storage.tasks().spawn_reserved(id, f(Context::new(storage)));
        core::mem::forget(self);
        JoinHandle::new(storage.tasks(), id)
    }
}

impl<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Drop
    for TaskReservation<
        '_,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn drop(&mut self) {
        self.storage.tasks().release(self.id);
    }
}

#[cfg(test)]
mod tests {
    use core::{future::pending, time::Duration};

    use crate::{
        Selector,
        testing::{EventLoopTestingExt, Log, TEST_MAX_TASKS_COUNT, TestLoop, TestStorage},
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn spawning_runs_the_task() {
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async |ctx| {
                let task = ctx.reserve_task().await;
                task.spawn(async |ctx| ctx.dispatch(1)).await;
                ctx.dispatch(2);
                pending::<()>().await;
            },
        )
        .unwrap();
        assert_eq!(event_loop.state_mut().events(), [1, 2]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn dropping_frees_the_slot_and_wakes_the_waiter() {
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async |ctx| {
                for _ in 0..TEST_MAX_TASKS_COUNT - 3 {
                    ctx.spawn(async |_| pending::<()>().await);
                }
                let task = ctx.reserve_task().await;
                ctx.spawn(async |ctx| {
                    let _task = ctx.reserve_task().await;
                    ctx.dispatch(2);
                    pending::<()>().await;
                });
                ctx.sleep(Duration::from_millis(1)).await;
                ctx.dispatch(1);
                drop(task);
                pending::<()>().await;
            },
        )
        .unwrap();
        assert!(event_loop.state_mut().events().is_empty());
        event_loop.run_until_events(2);
        assert_eq!(event_loop.state_mut().events(), [1, 2]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    #[should_panic(expected = "more than 16 concurrent tasks")]
    fn a_reserved_slot_is_not_handed_to_spawn() {
        let mut selector = Selector::new().unwrap();
        let _ = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async |ctx| {
                for _ in 0..TEST_MAX_TASKS_COUNT - 2 {
                    ctx.spawn(async |_| pending::<()>().await);
                }
                let _task = ctx.reserve_task().await;
                ctx.spawn(async |_| {});
            },
        );
    }
}
