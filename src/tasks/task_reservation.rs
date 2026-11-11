use crate::{
    context::Context,
    state::State,
    storage::LoopStorageGuard,
    tasks::{JoinHandle, TaskEntry, TaskId},
};

/// A task reservation that can be used to spawn a task without waiting for a free slot.
#[must_use = "a reservation holds a task slot until it is spawned into or dropped"]
pub struct TaskReservation<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    storage: LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    id: TaskId,
    entry: &'static TaskEntry,
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> TaskReservation<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    pub(crate) fn new(
        storage: LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        id: TaskId,
        entry: &'static TaskEntry,
    ) -> Self {
        Self { storage, id, entry }
    }

    /// Spawns a task using a reserved slot that is held by this [`TaskReservation`].
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// 1. the future is bigger than `TASK_SIZE` bytes
    /// 2. it is aligned to more than 16 bytes.
    pub fn spawn<F, Fut>(self, f: F) -> JoinHandle<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
    where
        F: FnOnce(Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let Self { storage, id, entry } = self;
        storage
            .tasks()
            .spawn_reserved(id, entry, f(Context::new(storage)));
        core::mem::forget(self);
        JoinHandle::new(storage.tasks(), id)
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Drop for TaskReservation<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn drop(&mut self) {
        self.storage.tasks().release(self.id);
    }
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for TaskReservation<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("TaskReservation")
    }
}

#[cfg(test)]
mod tests {
    use core::{cell::RefCell, future::pending, time::Duration};

    use crate::{
        Selector,
        testing::{EventLoopTestingExt, Log, TEST_TASKS, TestLoop, TestStorage},
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn spawning_runs_the_task() {
        let mut selector = Selector::new().unwrap();
        let mut state = RefCell::new(Log::default());
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
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
        let mut state = RefCell::new(Log::default());
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async |ctx| {
                for _ in 0..TEST_TASKS - 3 {
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
        let mut state = RefCell::new(Log::default());
        let _ = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async |ctx| {
                for _ in 0..TEST_TASKS - 2 {
                    ctx.spawn(async |_| pending::<()>().await);
                }
                let _task = ctx.reserve_task().await;
                ctx.spawn(async |_| {});
            },
        );
    }
}
