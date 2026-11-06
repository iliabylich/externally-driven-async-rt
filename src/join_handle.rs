use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use crate::storage::{Storage, TaskId};
use crate::task::JoinStatus;

pub struct JoinHandle<'ctx> {
    storage: &'ctx RefCell<Storage>,
    id: TaskId,
}

impl<'ctx> JoinHandle<'ctx> {
    pub(crate) fn new(storage: &'ctx RefCell<Storage>, id: TaskId) -> Self {
        Self { storage, id }
    }
}

impl Future for JoinHandle<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        match self
            .storage
            .borrow_mut()
            .set_join_waiter(self.id, cx.waker())
        {
            JoinStatus::Running => Poll::Pending,
            JoinStatus::Finished => Poll::Ready(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::pending;

    use crate::Error;
    use crate::EventLoop;
    use crate::test_utils::{
        Log, TEST_MAX_IO, TEST_MAX_TASKS, TEST_MAX_TIMERS, pair, readable, wake_by_writing,
    };

    #[test]
    fn joining_a_running_task_waits_for_it() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                ctx.spawn(async move |ctx| {
                    readable(ctx, &a).await;
                    ctx.dispatch(1);
                })
                .await;
                ctx.dispatch(2);
            },
        )
        .unwrap();
        assert!(event_loop.as_mut().state_mut().0.is_empty());
        wake_by_writing(&*event_loop, &b);
        assert_eq!(
            event_loop.as_mut().drain_events_without_blocking(),
            Err(Error::RootFinished)
        );
        assert_eq!(event_loop.as_mut().state_mut().0, vec![1, 2]);
    }

    #[test]
    fn joining_a_finished_task_resolves_immediately() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let handle = ctx.spawn(async |ctx| ctx.dispatch(1));
                readable(ctx, &a).await;
                handle.await;
                ctx.dispatch(2);
            },
        )
        .unwrap();
        assert_eq!(event_loop.as_mut().state_mut().0, vec![1]);
        wake_by_writing(&*event_loop, &b);
        assert_eq!(
            event_loop.as_mut().drain_events_without_blocking(),
            Err(Error::RootFinished)
        );
        assert_eq!(event_loop.as_mut().state_mut().0, vec![1, 2]);
    }

    #[test]
    fn dropped_handle_detaches_the_task() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                ctx.spawn(async move |ctx| {
                    readable(ctx, &a).await;
                    ctx.dispatch(1);
                });
                pending::<()>().await;
            },
        )
        .unwrap();
        wake_by_writing(&*event_loop, &b);
        assert_eq!(event_loop.as_mut().drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.as_mut().state_mut().0, vec![1]);
    }

    #[test]
    fn spawned_task_can_spawn_and_join() {
        let event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
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
        assert_eq!(event_loop.unwrap().as_mut().state_mut().0, vec![1, 2, 3]);
    }
}
