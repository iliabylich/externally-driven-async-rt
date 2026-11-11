use core::{future::Future, pin::Pin, task::Poll};

use crate::{context::Context, state::State, tasks::TaskReservation};

/// A future to obtain a [`TaskReservation`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReserveTask<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    ctx: Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    waiting: bool,
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> ReserveTask<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    pub(crate) fn new(ctx: Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>) -> Self {
        Self {
            ctx,
            waiting: false,
        }
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
> Future for ReserveTask<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    type Output = TaskReservation<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>;

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let ctx = this.ctx;
        match ctx.reserve() {
            Some((id, entry)) => {
                if this.waiting {
                    ctx.clear_slot_waiter();
                    this.waiting = false;
                }
                Poll::Ready(TaskReservation::new(this.ctx, id, entry))
            }
            None => {
                assert!(
                    this.waiting || !ctx.has_slot_waiter(),
                    "only one task can wait for a free task slot"
                );
                ctx.set_slot_waiter(cx.waker());
                this.waiting = true;
                Poll::Pending
            }
        }
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Drop for ReserveTask<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn drop(&mut self) {
        if self.waiting {
            self.ctx.clear_slot_waiter();
        }
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
> core::fmt::Debug for ReserveTask<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("ReserveTask")
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
    fn waits_until_a_task_finishes() {
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
                ctx.spawn(async |ctx| {
                    ctx.sleep(Duration::from_millis(10)).await;
                    ctx.dispatch(1);
                });
                for _ in 0..TEST_TASKS - 2 {
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
        let mut state = RefCell::new(Log::default());
        let _ = TestLoop::new(
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
