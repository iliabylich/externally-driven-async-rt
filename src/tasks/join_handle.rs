use core::{future::Future, pin::Pin, task::Poll};

use crate::{
    context::Context,
    state::State,
    tasks::{JoinStatus, TaskId},
};

/// A join handle of the spawned task, resolves once the task is finished.
pub struct JoinHandle<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    ctx: Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    pub(crate) id: TaskId<TASK_SIZE>,
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> JoinHandle<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    pub(crate) fn new(
        ctx: Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        id: TaskId<TASK_SIZE>,
    ) -> Self {
        Self { ctx, id }
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Future for JoinHandle<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        match self.ctx.storage.tasks.set_join_waiter(self.id, cx.waker()) {
            JoinStatus::Running => Poll::Pending,
            JoinStatus::Finished => Poll::Ready(()),
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
> core::fmt::Debug for JoinHandle<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("JoinHandle")
    }
}

#[cfg(test)]
mod tests {
    use core::{cell::RefCell, future::pending};

    use crate::{
        Error, Selector,
        testing::{EventLoopTestingExt, Log, TestLoop, TestStorage, socketpair, wait_readable},
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn joining_a_running_task_waits_for_it() {
        let (rx, tx) = socketpair();
        let mut selector = Selector::new().unwrap();
        let mut state = RefCell::new(Log::default());
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async move |ctx| {
                ctx.spawn(async move |ctx| {
                    wait_readable(&ctx, &rx).await;
                    ctx.dispatch(1);
                })
                .await;
                ctx.dispatch(2);
            },
        )
        .unwrap();
        assert!(event_loop.state_mut().events().is_empty());
        event_loop.wake_by_writing(&tx);
        assert_eq!(
            event_loop.drain_events_without_blocking(),
            Err(Error::RootFinished)
        );
        assert_eq!(event_loop.state_mut().events(), [1, 2]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn joining_a_finished_task_resolves_immediately() {
        let (rx, tx) = socketpair();
        let mut selector = Selector::new().unwrap();
        let mut state = RefCell::new(Log::default());
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async move |ctx| {
                let handle = ctx.spawn(async |ctx| ctx.dispatch(1));
                wait_readable(&ctx, &rx).await;
                handle.await;
                ctx.dispatch(2);
            },
        )
        .unwrap();
        assert_eq!(event_loop.state_mut().events(), [1]);
        event_loop.wake_by_writing(&tx);
        assert_eq!(
            event_loop.drain_events_without_blocking(),
            Err(Error::RootFinished)
        );
        assert_eq!(event_loop.state_mut().events(), [1, 2]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn dropped_handle_detaches_the_task() {
        let (rx, tx) = socketpair();
        let mut selector = Selector::new().unwrap();
        let mut state = RefCell::new(Log::default());
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async move |ctx| {
                ctx.spawn(async move |ctx| {
                    wait_readable(&ctx, &rx).await;
                    ctx.dispatch(1);
                });
                pending::<()>().await;
            },
        )
        .unwrap();
        event_loop.wake_by_writing(&tx);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().events(), [1]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn spawned_task_can_spawn_and_join() {
        let mut selector = Selector::new().unwrap();
        let mut state = RefCell::new(Log::default());
        let event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async |ctx| {
                ctx.spawn(async |ctx| {
                    ctx.spawn(async |ctx| ctx.dispatch(1)).await;
                    ctx.dispatch(2);
                })
                .await;
                ctx.dispatch(3);
                pending::<()>().await;
            },
        );
        assert_eq!(event_loop.unwrap().state_mut().events(), [1, 2, 3]);
    }
}
