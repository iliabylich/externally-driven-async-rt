use core::{future::Future, pin::Pin, task::Poll};

use crate::{state::State, storage::LoopStorageGuard, tasks::TaskReservation};

/// A future to obtain a [`TaskReservation`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReserveTask<
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
    waiting: bool,
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
    ReserveTask<
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
        Self {
            storage,
            waiting: false,
        }
    }
}

impl<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Future
    for ReserveTask<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    type Output = TaskReservation<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >;

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let tasks = this.storage.tasks();
        match tasks.reserve() {
            Some(id) => {
                if this.waiting {
                    tasks.clear_slot_waiter();
                    this.waiting = false;
                }
                Poll::Ready(TaskReservation::new(this.storage, id))
            }
            None => {
                assert!(
                    this.waiting || !tasks.has_slot_waiter(),
                    "only one task can wait for a free task slot"
                );
                tasks.set_slot_waiter(cx.waker());
                this.waiting = true;
                Poll::Pending
            }
        }
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
    for ReserveTask<
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
        if self.waiting {
            self.storage.tasks().clear_slot_waiter();
        }
    }
}

impl<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> core::fmt::Debug
    for ReserveTask<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("ReserveTask")
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
    fn waits_until_a_task_finishes() {
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async |ctx| {
                ctx.spawn(async |ctx| {
                    ctx.sleep(Duration::from_millis(10)).await;
                    ctx.dispatch(1);
                });
                for _ in 0..TEST_MAX_TASKS_COUNT - 2 {
                    ctx.spawn(async |_| pending::<()>().await);
                }
                let _task = ctx.reserve_task().await;
                ctx.dispatch(2);
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
    #[should_panic(expected = "only one task can wait for a free task slot")]
    fn a_second_waiter_panics() {
        let mut selector = Selector::new().unwrap();
        let _ = TestLoop::new(
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
                for _ in 0..2 {
                    ctx.spawn(async |ctx| {
                        let _task = ctx.reserve_task().await;
                    });
                }
                pending::<()>().await;
            },
        );
    }
}
