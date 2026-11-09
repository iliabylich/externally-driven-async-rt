use core::{future::Future, pin::Pin, task::Poll};

use crate::{
    instant::Instant,
    storage::{TimerIndex, Timers},
};

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Sleep<'ctx, const MAX_TIMERS_COUNT: usize> {
    timers: &'ctx Timers<MAX_TIMERS_COUNT>,
    deadline: Instant,
    timer: Option<TimerIndex>,
}

impl<'ctx, const MAX_TIMERS_COUNT: usize> Sleep<'ctx, MAX_TIMERS_COUNT> {
    pub(crate) fn new(timers: &'ctx Timers<MAX_TIMERS_COUNT>, deadline: Instant) -> Self {
        Self {
            timers,
            deadline,
            timer: None,
        }
    }
}

impl<const MAX_TIMERS_COUNT: usize> Future for Sleep<'_, MAX_TIMERS_COUNT> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            if let Some(timer) = self.timer.take() {
                self.timers.release(timer);
            }
            return Poll::Ready(());
        }
        match self.timer {
            Some(timer) => self.timers.set_waker(timer, cx.waker()),
            None => self.timer = Some(self.timers.alloc(self.deadline, cx.waker().clone())),
        }
        Poll::Pending
    }
}

impl<const MAX_TIMERS_COUNT: usize> Drop for Sleep<'_, MAX_TIMERS_COUNT> {
    fn drop(&mut self) {
        if let Some(timer) = self.timer {
            self.timers.release(timer);
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
        instant::Instant,
        sleep::Sleep,
        storage::Timers,
        test_utils::{
            Log, TestLoop, TestStorage, is_readable, run_until_root_finishes, sleep_blocking,
        },
        wake_queue::WakeQueue,
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
            Log::default(),
            async |ctx| {
                ctx.sleep(Duration::from_millis(20)).await;
                ctx.dispatch(1);
            },
        )
        .unwrap();
        assert!(event_loop.state_mut().events().is_empty());
        run_until_root_finishes(&mut event_loop);
        assert!(Instant::now().saturating_duration_since(start) >= Duration::from_millis(20));
        assert_eq!(event_loop.state_mut().events(), [1]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn sleeps_complete_in_deadline_order() {
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async |ctx| {
                let handles = [30, 10, 20].map(|ms| {
                    ctx.spawn(async move |ctx| {
                        ctx.sleep(Duration::from_millis(ms)).await;
                        ctx.dispatch(ms as u32);
                    })
                });
                for handle in handles {
                    handle.await;
                }
            },
        )
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().events(), [10, 20, 30]);
        assert!(!is_readable(&event_loop, 0));
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
            Log::default(),
            async move |ctx| {
                let mut next = Instant::now();
                for i in 0..5 {
                    next = next + period;
                    ctx.sleep(next.saturating_duration_since(Instant::now()))
                        .await;
                    ctx.dispatch(i);
                }
            },
        )
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().events(), [0, 1, 2, 3, 4]);
        assert!(Instant::now().saturating_duration_since(start) >= period * 5);
    }

    #[test]
    fn sleep_until_now_is_ready_without_a_timer() {
        let timers = &Timers::<1>::new();
        let mut cx = Context::from_waker(Waker::noop());
        let mut sleep = Sleep::new(timers, Instant::now());
        assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Ready(()));
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn dropping_a_pending_sleep_releases_its_timer() {
        let timers = &Timers::<1>::new();
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now() + Duration::from_secs(60);
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
        let mut sleep = Sleep::new(timers, Instant::now() + Duration::from_millis(1));
        assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Pending);
        sleep_blocking(Duration::from_millis(1));
        assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Ready(()));
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn repolling_a_pending_sleep_replaces_its_waker() {
        static WAKERS: WakeQueue<2> = WakeQueue::new();
        let timers = &Timers::<1>::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut sleep = Sleep::new(timers, deadline);
        for id in [0, 1] {
            let waker = WAKERS.waker(id);
            let mut cx = Context::from_waker(&waker);
            assert_eq!(Pin::new(&mut sleep).poll(&mut cx), Poll::Pending);
        }
        let woken = timers.take_expired(deadline).unwrap();
        assert_eq!(WAKERS.id_by_waker_data(&woken), 1);
    }
}
