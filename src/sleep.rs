use core::{future::Future, pin::Pin, task::Poll};

use crate::{
    instant::Instant,
    storage::{TimerIndex, Timers},
};

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Sleep<'ctx> {
    timers: &'ctx Timers,
    deadline: Instant,
    timer: Option<TimerIndex>,
}

impl<'ctx> Sleep<'ctx> {
    pub(crate) fn new(timers: &'ctx Timers, deadline: Instant) -> Self {
        Self {
            timers,
            deadline,
            timer: None,
        }
    }
}

impl Future for Sleep<'_> {
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

impl Drop for Sleep<'_> {
    fn drop(&mut self) {
        if let Some(timer) = self.timer {
            self.timers.release(timer);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{future::pending, pin::pin, time::Duration};

    use crate::{
        Capacities,
        instant::Instant,
        test_utils::{
            First, Log, TEST_CAPACITIES, Winner, has_timers, is_readable, pair, readable,
            run_until_root_finishes, test_loop, wake_by_writing,
        },
    };

    #[test]
    fn zero_sleep_completes_immediately() {
        let event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async |ctx| {
            ctx.sleep(Duration::ZERO).await;
            ctx.dispatch(1);
            pending::<()>().await;
        },);
        let mut event_loop = event_loop.unwrap();
        assert_eq!(event_loop.state_mut().events(), [1]);
        assert!(!has_timers(&event_loop));
    }

    #[test]
    fn sleep_waits_at_least_its_duration() {
        let start = Instant::now();
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async |ctx| {
            ctx.sleep(Duration::from_millis(20)).await;
            ctx.dispatch(1);
        },)
        .unwrap();
        assert!(event_loop.state_mut().events().is_empty());
        run_until_root_finishes(&mut event_loop);
        assert!(Instant::now().saturating_duration_since(start) >= Duration::from_millis(20));
        assert_eq!(event_loop.state_mut().events(), [1]);
    }

    #[test]
    fn sleeps_complete_in_deadline_order() {
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async |ctx| {
            let handles = [30, 10, 20].map(|ms| {
                ctx.spawn(async move |ctx| {
                    ctx.sleep(Duration::from_millis(ms)).await;
                    ctx.dispatch(ms as u32);
                })
            });
            for handle in handles {
                handle.await;
            }
        },)
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().events(), [10, 20, 30]);
        assert!(!is_readable(&event_loop, 0));
    }

    #[test]
    fn many_concurrent_sleeps() {
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async |ctx| {
            let handles: [_; 12] = core::array::from_fn(|i| {
                ctx.spawn(async move |ctx| {
                    ctx.sleep(Duration::from_millis(1 + i as u64 % 10)).await;
                    ctx.dispatch(1);
                })
            });
            for handle in handles {
                handle.await;
            }
        },)
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().events().len(), 12);
    }

    #[test]
    fn losing_sleep_leaves_no_timer_behind() {
        let (a, b) = pair();
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async move |ctx| {
            let winner = First(
                pin!(readable(ctx, &a)),
                ctx.sleep(Duration::from_millis(50)),
            )
            .await;
            assert_eq!(winner, Winner::A);
            ctx.dispatch(1);
            pending::<()>().await;
        },)
        .unwrap();
        assert!(has_timers(&event_loop));
        wake_by_writing(&event_loop, &b);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().events(), [1]);
        assert!(!has_timers(&event_loop));
        assert!(
            !is_readable(&event_loop, 100),
            "the OS timer is still armed"
        );
    }

    #[test]
    fn winning_sleep_drops_the_other_future() {
        let (a, b) = pair();
        // The same socket, for checking it from outside the loop.
        let a_outside = rustix::io::dup(&a).unwrap();
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async move |ctx| {
            let winner = First(
                pin!(readable(ctx, &a)),
                ctx.sleep(Duration::from_millis(10)),
            )
            .await;
            assert_eq!(winner, Winner::B);
            ctx.dispatch(1);
            pending::<()>().await;
        },)
        .unwrap();
        while event_loop.state_mut().events().is_empty() {
            assert!(is_readable(&event_loop, 2000));
            assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        }
        rustix::io::write(&b, b"x").unwrap();
        assert!(!is_readable(&event_loop, 0));
        assert!(is_readable(&a_outside, 0));
    }

    #[test]
    fn drift_free_interval_from_relative_sleeps() {
        let period = Duration::from_millis(10);
        let start = Instant::now();
        let mut event_loop = test_loop!(Log::default(), TEST_CAPACITIES, async move |ctx| {
            let mut next = Instant::now();
            for i in 0..5 {
                next = next + period;
                ctx.sleep(next.saturating_duration_since(Instant::now()))
                    .await;
                ctx.dispatch(i);
            }
        },)
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().events(), [0, 1, 2, 3, 4]);
        assert!(Instant::now().saturating_duration_since(start) >= period * 5);
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent sleeps")]
    fn exceeding_max_timers_count_panics() {
        let _ = test_loop!(
            Log::default(),
            Capacities {
                max_timers: 1,
                ..TEST_CAPACITIES
            },
            async |ctx| {
                let long = Duration::from_secs(60);
                First(ctx.sleep(long), ctx.sleep(long)).await;
            },
        );
    }

    #[test]
    fn sequential_sleeps_reuse_the_timer_slot() {
        let mut event_loop = test_loop!(
            Log::default(),
            Capacities {
                max_timers: 1,
                ..TEST_CAPACITIES
            },
            async |ctx| {
                for i in 0..3 {
                    ctx.sleep(Duration::from_millis(1)).await;
                    ctx.dispatch(i);
                }
                pending::<()>().await;
            },
        )
        .unwrap();
        while event_loop.state_mut().events().len() < 3 {
            assert!(is_readable(&event_loop, 2000));
            assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        }
        assert_eq!(event_loop.state_mut().events(), [0, 1, 2]);
    }
}
