use core::{future::Future, pin::Pin, task::Poll};

use crate::time::{Instant, TimerEntry, TimerIndex, Timers};

/// A sleep future.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Sleep<'ctx, const TIMERS: usize> {
    timers: &'ctx Timers<TIMERS>,
    deadline: Instant,
    timer: Option<(TimerIndex, &'ctx TimerEntry)>,
}

impl<'ctx, const TIMERS: usize> Sleep<'ctx, TIMERS> {
    pub(crate) fn new(timers: &'ctx Timers<TIMERS>, deadline: Instant) -> Self {
        Self {
            timers,
            deadline,
            timer: None,
        }
    }
}

impl<const TIMERS: usize> Future for Sleep<'_, TIMERS> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            if let Some((index, _)) = self.timer.take() {
                self.timers.release(index);
            }
            return Poll::Ready(());
        }
        match self.timer {
            Some((_, timer)) => timer.set_waker(cx.waker()),
            None => self.timer = Some(self.timers.alloc(self.deadline, cx.waker().clone())),
        }
        Poll::Pending
    }
}

impl<const TIMERS: usize> Drop for Sleep<'_, TIMERS> {
    fn drop(&mut self) {
        if let Some((index, _)) = self.timer {
            self.timers.release(index);
        }
    }
}

impl<const TIMERS: usize> core::fmt::Debug for Sleep<'_, TIMERS> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Sleep")
    }
}

#[cfg(test)]
mod tests {
    use core::{
        cell::RefCell,
        pin::Pin,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use crate::{
        Selector,
        tasks::WakeQueue,
        testing::{EventLoopTestingExt, Log, TestLoop, TestStorage, is_readable, sleep_blocking},
        time::{Instant, Sleep, Timers},
    };

    #[test]
    #[cfg_attr(miri, ignore)]
    fn sleep_waits_at_least_its_duration() {
        let start = Instant::now();
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
                ctx.sleep(Duration::from_millis(20)).await;
                ctx.dispatch(1);
            },
        )
        .unwrap();
        assert!(event_loop.state_mut().events().is_empty());
        event_loop.run_until_root_finishes();
        assert!(Instant::now().saturating_duration_since(start) >= Duration::from_millis(20));
        assert_eq!(event_loop.state_mut().events(), [1]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn sleeps_complete_in_deadline_order() {
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
        event_loop.run_until_root_finishes();
        assert_eq!(event_loop.state_mut().events(), [10, 20, 30]);
        assert!(!is_readable(&event_loop, 0));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn drift_free_interval_from_relative_sleeps() {
        let period = Duration::from_millis(10);
        let start = Instant::now();
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
        event_loop.run_until_root_finishes();
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
