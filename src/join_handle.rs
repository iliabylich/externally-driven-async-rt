use core::{future::Future, pin::Pin, task::Poll};

use crate::{
    storage::{TaskId, TasksGuard},
    task::JoinStatus,
};

pub struct JoinHandle<
    'a,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
> {
    tasks: TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
    id: TaskId,
}

impl<'a, const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize>
    JoinHandle<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
    pub(crate) fn new(
        tasks: TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
        id: TaskId,
    ) -> Self {
        Self { tasks, id }
    }
}

impl<const MAX_TASKS_COUNT: usize, const MAX_TASK_SIZE: usize, const MAX_ROOT_TASK_SIZE: usize>
    Future for JoinHandle<'_, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        match self.tasks.set_join_waiter(self.id, cx.waker()) {
            JoinStatus::Running => Poll::Pending,
            JoinStatus::Finished => Poll::Ready(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::pending;

    use crate::{
        Error, Selector,
        test_utils::{Log, TestLoop, TestStorage, socketpair, wait_readable, wake_by_writing},
    };

    #[test]
    fn joining_a_running_task_waits_for_it() {
        let (a, b) = socketpair();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async move |ctx| {
                ctx.spawn(async move |ctx| {
                    wait_readable(ctx, &a).await;
                    ctx.dispatch(1);
                })
                .await;
                ctx.dispatch(2);
            },
        )
        .unwrap();
        assert!(event_loop.state_mut().events().is_empty());
        wake_by_writing(&event_loop, &b);
        assert_eq!(
            event_loop.drain_events_without_blocking(),
            Err(Error::RootFinished)
        );
        assert_eq!(event_loop.state_mut().events(), [1, 2]);
    }

    #[test]
    fn joining_a_finished_task_resolves_immediately() {
        let (a, b) = socketpair();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async move |ctx| {
                let handle = ctx.spawn(async |ctx| ctx.dispatch(1));
                wait_readable(ctx, &a).await;
                handle.await;
                ctx.dispatch(2);
            },
        )
        .unwrap();
        assert_eq!(event_loop.state_mut().events(), [1]);
        wake_by_writing(&event_loop, &b);
        assert_eq!(
            event_loop.drain_events_without_blocking(),
            Err(Error::RootFinished)
        );
        assert_eq!(event_loop.state_mut().events(), [1, 2]);
    }

    #[test]
    fn dropped_handle_detaches_the_task() {
        let (a, b) = socketpair();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async move |ctx| {
                ctx.spawn(async move |ctx| {
                    wait_readable(ctx, &a).await;
                    ctx.dispatch(1);
                });
                pending::<()>().await;
            },
        )
        .unwrap();
        wake_by_writing(&event_loop, &b);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().events(), [1]);
    }

    #[test]
    fn spawned_task_can_spawn_and_join() {
        let mut selector = Selector::new().unwrap();
        let event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
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
