use core::{cell::Cell, marker::PhantomData};

use crate::{
    context::Context,
    state::State,
    tasks::{JoinHandle, TaskEntry, TaskId, Tasks},
};

/// A task reservation that can be used to spawn a task without waiting for a free slot.
#[must_use = "a reservation holds a task slot until it is spawned into or dropped"]
pub struct TaskReservation<
    'a,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
> {
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    id: TaskId<TASK_SIZE>,
    entry: &'static TaskEntry<TASK_SIZE>,
    _invariant_not_send: PhantomData<(Cell<&'a ()>, *const ())>,
}

impl<'a, const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
    TaskReservation<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    pub(crate) fn new<S: State, const TIMERS: usize, const FDS: usize>(
        ctx: Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        id: TaskId<TASK_SIZE>,
        entry: &'static TaskEntry<TASK_SIZE>,
    ) -> Self {
        Self {
            tasks: &ctx.storage.tasks,
            id,
            entry,
            _invariant_not_send: PhantomData,
        }
    }

    /// Spawns a task using a reserved slot that is held by this [`TaskReservation`].
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// 1. the future is bigger than `TASK_SIZE` bytes
    /// 2. it is aligned to more than 16 bytes.
    pub fn spawn<Fut>(self, future: Fut) -> JoinHandle<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
    where
        Fut: Future<Output = ()> + 'a,
    {
        let Self {
            tasks, id, entry, ..
        } = self;
        // SAFETY: `Fut: 'a`, where `'a` is the invariant lifetime of the `Context` this
        // reservation was made from, so the task outlives every poll and drop as in
        // `Context::spawn`.
        unsafe { tasks.spawn_reserved(id, entry, future) };
        core::mem::forget(self);
        JoinHandle::new(tasks, id)
    }
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> Drop
    for TaskReservation<'_, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    fn drop(&mut self) {
        self.tasks.release(self.id);
    }
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> core::fmt::Debug
    for TaskReservation<'_, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
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
                task.spawn(async move { ctx.dispatch(1) }).await;
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
