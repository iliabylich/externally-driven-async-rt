use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use crate::task::{JoinStatus, TaskId, Tasks};

pub struct JoinHandle<'ctx> {
    tasks: &'ctx RefCell<Tasks>,
    id: TaskId,
}

impl<'ctx> JoinHandle<'ctx> {
    pub(crate) fn new(tasks: &'ctx RefCell<Tasks>, id: TaskId) -> Self {
        Self { tasks, id }
    }
}

impl Future for JoinHandle<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        match self.tasks.borrow_mut().set_join_waiter(self.id, cx.waker()) {
            JoinStatus::Running => Poll::Pending,
            JoinStatus::Finished => Poll::Ready(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::pending;

    use crate::test_utils::{Log, pair, readable, wake_by_writing};
    use crate::{Error, EventLoop};

    #[test]
    fn joining_a_running_task_waits_for_it() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            ctx.spawn(async move |ctx| {
                readable(ctx, &a).await;
                ctx.dispatch(1);
            })
            .await;
            ctx.dispatch(2);
        })
        .unwrap();
        assert!(event_loop.state_mut().0.is_empty());
        wake_by_writing(&event_loop, &b);
        assert_eq!(
            event_loop.drain_events_without_blocking(),
            Err(Error::RootFinished)
        );
        assert_eq!(event_loop.state_mut().0, vec![1, 2]);
    }

    #[test]
    fn joining_a_finished_task_resolves_immediately() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let handle = ctx.spawn(async |ctx| ctx.dispatch(1));
            readable(ctx, &a).await;
            handle.await;
            ctx.dispatch(2);
        })
        .unwrap();
        assert_eq!(event_loop.state_mut().0, vec![1]);
        wake_by_writing(&event_loop, &b);
        assert_eq!(
            event_loop.drain_events_without_blocking(),
            Err(Error::RootFinished)
        );
        assert_eq!(event_loop.state_mut().0, vec![1, 2]);
    }

    #[test]
    fn dropped_handle_detaches_the_task() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            ctx.spawn(async move |ctx| {
                readable(ctx, &a).await;
                ctx.dispatch(1);
            });
            pending::<()>().await;
        })
        .unwrap();
        wake_by_writing(&event_loop, &b);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().0, vec![1]);
    }

    #[test]
    fn spawned_task_can_spawn_and_join() {
        let event_loop = EventLoop::new(Log::default(), async |ctx| {
            ctx.spawn(async |ctx| {
                ctx.spawn(async |ctx| ctx.dispatch(1)).await;
                ctx.dispatch(2);
            })
            .await;
            ctx.dispatch(3);
            pending::<()>().await;
        });
        assert_eq!(event_loop.unwrap().state_mut().0, vec![1, 2, 3]);
    }
}
