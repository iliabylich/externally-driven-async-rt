use core::{future::Future, pin::Pin, task::Poll};

use crate::time::Sleep;

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Timeout<'ctx, F, const MAX_TIMERS_COUNT: usize> {
    future: F,
    sleep: Sleep<'ctx, MAX_TIMERS_COUNT>,
}

impl<'ctx, F, const MAX_TIMERS_COUNT: usize> Timeout<'ctx, F, MAX_TIMERS_COUNT> {
    pub(crate) fn new(future: F, sleep: Sleep<'ctx, MAX_TIMERS_COUNT>) -> Self {
        Self { future, sleep }
    }
}

impl<F: Future, const MAX_TIMERS_COUNT: usize> Future for Timeout<'_, F, MAX_TIMERS_COUNT> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `future` is pinned structurally: `Timeout` never moves it, has no `Drop`, and is
        // `Unpin` only when `F` is. `sleep` is not pinned structurally, which is fine as `Sleep` is
        // `Unpin`.
        let (future, sleep) = unsafe {
            let this = self.get_unchecked_mut();
            (Pin::new_unchecked(&mut this.future), &mut this.sleep)
        };
        if let Poll::Ready(output) = future.poll(cx) {
            return Poll::Ready(Some(output));
        }
        Pin::new(sleep).poll(cx).map(|()| None)
    }
}

#[cfg(test)]
mod tests {
    use core::{
        future::{Future, pending},
        pin::pin,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use crate::{
        Selector,
        testing::{EventLoopTestingExt, Log, TestLoop, TestStorage, socketpair, wait_readable},
        time::{Instant, Sleep, Timeout, Timers},
    };

    fn registrations(event_loop: &TestLoop<Log>) -> usize {
        event_loop.storage.io_waiters().count()
    }

    fn has_timers(event_loop: &TestLoop<Log>) -> bool {
        event_loop.storage.timers().next_deadline().is_some()
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn future_finishing_first_wins() {
        let (rx, tx) = socketpair();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async move |ctx| {
                let result = ctx
                    .timeout(Duration::from_secs(60), wait_readable(ctx, &rx))
                    .await;
                ctx.dispatch(result.map_or(0, |()| 1));
                pending::<()>().await;
            },
        )
        .unwrap();
        assert!(has_timers(&event_loop));
        assert_eq!(registrations(&event_loop), 1);
        event_loop.wake_by_writing(&tx);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().events(), [1]);
        assert!(!has_timers(&event_loop));
        assert_eq!(registrations(&event_loop), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn elapsing_drops_the_future() {
        let (rx, _tx) = socketpair();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async move |ctx| {
                let result = ctx
                    .timeout(Duration::from_millis(10), wait_readable(ctx, &rx))
                    .await;
                ctx.dispatch(result.map_or(0, |()| 1));
                pending::<()>().await;
            },
        )
        .unwrap();
        event_loop.run_until_events(1);
        assert_eq!(event_loop.state_mut().events(), [0]);
        assert!(!has_timers(&event_loop));
        assert_eq!(registrations(&event_loop), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn ready_future_beats_a_zero_timeout() {
        let (rx, tx) = socketpair();
        rustix::io::write(&tx, b"x").unwrap();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async move |ctx| {
                let result = ctx.timeout(Duration::ZERO, wait_readable(ctx, &rx)).await;
                ctx.dispatch(result.map_or(0, |()| 1));
                pending::<()>().await;
            },
        )
        .unwrap();
        assert_eq!(event_loop.state_mut().events(), [1]);
        assert!(!has_timers(&event_loop));
    }

    #[test]
    fn ready_pinned_future_resolves_and_releases_the_timer() {
        let timers = &Timers::<1>::new();
        let mut cx = Context::from_waker(Waker::noop());
        let sleep = Sleep::new(timers, Instant::now() + Duration::from_secs(60));
        let mut timeout = pin!(Timeout::new(
            async {
                let value = 1;
                let borrowed = &value;
                core::future::ready(()).await;
                *borrowed
            },
            sleep,
        ));
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Ready(Some(1)));
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn pending_pinned_future_keeps_its_timer_until_dropped() {
        let timers = &Timers::<1>::new();
        let mut cx = Context::from_waker(Waker::noop());
        let sleep = Sleep::new(timers, Instant::now() + Duration::from_secs(60));
        {
            let mut timeout = pin!(Timeout::new(
                async {
                    let value = 1;
                    let borrowed = &value;
                    pending::<()>().await;
                    *borrowed
                },
                sleep,
            ));
            assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
            assert!(timers.next_deadline().is_some());
        }
        assert_eq!(timers.next_deadline(), None);
    }
}
