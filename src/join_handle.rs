use core::{future::Future, pin::Pin, task::Poll};

use crate::{
    state::State,
    storage::{Storage, TaskId},
    task::JoinStatus,
};

pub struct JoinHandle<'ctx, S: State> {
    storage: &'ctx Storage<S>,
    id: TaskId,
}

impl<'ctx, S: State> JoinHandle<'ctx, S> {
    pub(crate) fn new(storage: &'ctx Storage<S>, id: TaskId) -> Self {
        Self { storage, id }
    }
}

impl<S: State> Future for JoinHandle<'_, S> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        match self.storage.set_join_waiter(self.id, cx.waker()) {
            JoinStatus::Running => Poll::Pending,
            JoinStatus::Finished => Poll::Ready(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::pending;

    use crate::{
        Error,
        test_utils::{Log, TEST_CAPACITIES, pair, readable, test_loop, wake_by_writing},
    };

    #[test]
    fn joining_a_running_task_waits_for_it() {
        let (a, b) = pair();
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async move |ctx| {
            ctx.spawn(async move |ctx| {
                readable(ctx, &a).await;
                ctx.dispatch(1);
            })
            .await;
            ctx.dispatch(2);
        },)
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
        let (a, b) = pair();
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async move |ctx| {
            let handle = ctx.spawn(async |ctx| ctx.dispatch(1));
            readable(ctx, &a).await;
            handle.await;
            ctx.dispatch(2);
        },)
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
        let (a, b) = pair();
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async move |ctx| {
            ctx.spawn(async move |ctx| {
                readable(ctx, &a).await;
                ctx.dispatch(1);
            });
            pending::<()>().await;
        },)
        .unwrap();
        wake_by_writing(&event_loop, &b);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().events(), [1]);
    }

    #[test]
    fn spawned_task_can_spawn_and_join() {
        let event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async |ctx| {
            ctx.spawn(async |ctx| {
                ctx.spawn(async |ctx| ctx.dispatch(1)).await;
                ctx.dispatch(2);
            })
            .await;
            ctx.dispatch(3);
            pending::<()>().await;
        },);
        assert_eq!(event_loop.unwrap().state_mut().events(), [1, 2, 3]);
    }
}
