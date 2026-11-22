use core::{future::Future, pin::Pin, task::Poll};

use crate::time::{Instant, Timers, TimersFullError, timers::TimerIndex};

/// A sleep future.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Sleep<'ctx, const TIMERS: usize> {
    timers: &'ctx Timers<TIMERS>,
    deadline: Instant,
    timer: Option<TimerIndex>,
}

impl<'ctx, const TIMERS: usize> Sleep<'ctx, TIMERS> {
    pub(crate) const fn new(timers: &'ctx Timers<TIMERS>, deadline: Instant) -> Self {
        Self {
            timers,
            deadline,
            timer: None,
        }
    }
}

impl<const TIMERS: usize> Future for Sleep<'_, TIMERS> {
    type Output = Result<(), TimersFullError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.deadline {
            if let Some(index) = self.timer.take() {
                self.timers.release(index);
            }
            return Poll::Ready(Ok(()));
        }
        match self.timer {
            Some(index) => self.timers.set_waker(index, cx.waker()),
            None => match self.timers.alloc(self.deadline, cx.waker()) {
                Ok(index) => self.timer = Some(index),
                Err(err) => return Poll::Ready(Err(err)),
            },
        }
        Poll::Pending
    }
}

impl<const TIMERS: usize> Drop for Sleep<'_, TIMERS> {
    fn drop(&mut self) {
        if let Some(index) = self.timer {
            self.timers.release(index);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{
        pin::Pin,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use crate::{
        Selector,
        tasks::{RootStatus, WakeQueue},
        testing::{
            EventLoopTestingExt, TestContext, TestLoop, TestStorage, assert_yields, sleep_blocking,
            wake_expired_timers,
        },
        time::{Instant, Sleep, Timers, TimersFullError},
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn sleep_waits_at_least_its_duration() {
        let start = Instant::now();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            async |ctx| {
                ctx.sleep(Duration::from_millis(20)).await.unwrap();
                ctx.dispatch(1).unwrap();
            },
        )
        .unwrap();
        assert_yields!(event_loop.events(), []);
        event_loop.run_until_root_finishes();
        assert!(Instant::now().saturating_duration_since(start) >= Duration::from_millis(20));
        assert_yields!(event_loop.events(), [1]);
    }

    #[test]
    fn sleeps_complete_in_deadline_order() {
        let selector = Selector::new().unwrap();
        let (ctx, root): (TestContext<'_>, _) = {
            static STORAGE: TestStorage = TestStorage::new();
            STORAGE.claim(&selector).unwrap()
        };
        ctx.storage.tasks.spawn_root(root, ctx.claimed, async move {
            let handles = [300, 100, 200].map(|ms| {
                ctx.spawn(async move |ctx| {
                    ctx.sleep(Duration::from_millis(ms)).await.unwrap();
                    ctx.dispatch(u32::try_from(ms).unwrap()).unwrap();
                })
                .unwrap()
            });
            for handle in handles {
                handle.await;
            }
        });
        assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
        sleep_blocking(Duration::from_millis(300));
        wake_expired_timers(&ctx.storage.timers);
        assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Finished);
        assert_yields!(ctx.storage.events.iter_removing(), [100, 200, 300]);
        ctx.clear();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn drift_free_interval_from_relative_sleeps() {
        let period = Duration::from_millis(10);
        let start = Instant::now();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            async move |ctx| {
                let mut next = Instant::now();
                for i in 0..5 {
                    next = next.saturating_add(period);
                    ctx.sleep(next.saturating_duration_since(Instant::now()))
                        .await
                        .unwrap();
                    ctx.dispatch(i).unwrap();
                }
            },
        )
        .unwrap();
        event_loop.run_until_root_finishes();
        assert_yields!(event_loop.events(), [0, 1, 2, 3, 4]);
        assert!(Instant::now().saturating_duration_since(start) >= period * 5);
    }

    #[test]
    fn sleep_until_now_is_ready_without_a_timer() {
        let timers = &Timers::<1>::new();
        let mut cx = Context::from_waker(Waker::noop());
        let mut sleep = Sleep::new(timers, Instant::now());
        assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Ready(Ok(())));
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn dropping_a_pending_sleep_releases_its_timer() {
        let timers = &Timers::<1>::new();
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now().saturating_add(Duration::from_secs(60));
        let mut sleep = Sleep::new(timers, deadline);
        assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Pending);
        assert_eq!(timers.next_deadline(), Some(deadline));
        drop(sleep);
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn finished_sleep_releases_its_timer() {
        let timers = &Timers::<1>::new();
        let mut cx = Context::from_waker(Waker::noop());
        let mut sleep = Sleep::new(
            timers,
            Instant::now().saturating_add(Duration::from_millis(1)),
        );
        assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Pending);
        sleep_blocking(Duration::from_millis(1));
        assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Ready(Ok(())));
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn sleep_without_a_free_timer_fails() {
        let timers = &Timers::<1>::new();
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now().saturating_add(Duration::from_secs(60));
        let mut first = Sleep::new(timers, deadline);
        let mut second = Sleep::new(timers, deadline);
        assert_eq!(Pin::new(&mut first).poll(&mut cx), Poll::Pending);
        assert_eq!(
            Pin::new(&mut second).poll(&mut cx),
            Poll::Ready(Err(TimersFullError))
        );
    }

    #[test]
    fn repolling_a_pending_sleep_replaces_its_waker() {
        static WAKERS: WakeQueue<2> = WakeQueue::new();
        let timers = &Timers::<1>::new();
        let deadline = Instant::now().saturating_add(Duration::from_secs(60));
        let mut sleep = Sleep::new(timers, deadline);
        for idx in [0, 1] {
            let waker = WAKERS.waker(idx);
            let mut cx = Context::from_waker(&waker);
            assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Pending);
        }
        let woken = timers.take_expired(deadline).unwrap();
        assert_eq!(WAKERS.idx_by_waker_data(&woken), 1);
    }
}
