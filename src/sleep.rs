use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;
use std::time::Instant;

use crate::timers::{TimerKey, Timers};

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Sleep<'ctx> {
    timers: &'ctx RefCell<Timers>,
    deadline: Instant,
    key: Option<TimerKey>,
}

impl<'ctx> Sleep<'ctx> {
    pub(crate) fn new(timers: &'ctx RefCell<Timers>, deadline: Instant) -> Self {
        Self {
            timers,
            deadline,
            key: None,
        }
    }
}

impl Future for Sleep<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            if let Some(key) = self.key.take() {
                self.timers.borrow_mut().remove(key);
            }
            return Poll::Ready(());
        }
        match self.key {
            Some(key) => self.timers.borrow_mut().update_waker(key, cx.waker()),
            None => {
                let key = self
                    .timers
                    .borrow_mut()
                    .insert(self.deadline, cx.waker().clone());
                self.key = Some(key);
            }
        }
        Poll::Pending
    }
}

impl Drop for Sleep<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            self.timers.borrow_mut().remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::pending;
    use core::time::Duration;
    use std::rc::Rc;
    use std::time::Instant;

    use crate::EventLoop;
    use crate::test_utils::{
        First, Log, Winner, has_timers, is_readable, pair, readable, run_until_root_finishes,
        wake_by_writing,
    };

    #[test]
    fn sleep_with_a_past_deadline_completes_immediately() {
        let event_loop = EventLoop::new(Log::default(), async |ctx| {
            ctx.sleep_until(Instant::now()).await;
            ctx.sleep(Duration::ZERO).await;
            ctx.dispatch(1);
            pending::<()>().await;
        });
        let mut event_loop = event_loop.unwrap();
        assert_eq!(event_loop.state_mut().0, vec![1]);
        assert!(!has_timers(&event_loop));
    }

    #[test]
    fn sleep_waits_at_least_its_duration() {
        let start = Instant::now();
        let mut event_loop = EventLoop::new(Log::default(), async |ctx| {
            ctx.sleep(Duration::from_millis(20)).await;
            ctx.dispatch(1);
        })
        .unwrap();
        assert!(event_loop.state_mut().0.is_empty());
        run_until_root_finishes(&mut event_loop);
        assert!(start.elapsed() >= Duration::from_millis(20));
        assert_eq!(event_loop.state_mut().0, vec![1]);
    }

    #[test]
    fn sleeps_complete_in_deadline_order() {
        let mut event_loop = EventLoop::new(Log::default(), async |ctx| {
            let handles = [30, 10, 20].map(|ms| {
                ctx.spawn(async move |ctx| {
                    ctx.sleep(Duration::from_millis(ms)).await;
                    ctx.dispatch(ms as u32);
                })
            });
            for handle in handles {
                handle.await;
            }
        })
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().0, vec![10, 20, 30]);
        assert!(!is_readable(&event_loop, 0));
    }

    #[test]
    fn many_concurrent_sleeps() {
        let mut event_loop = EventLoop::new(Log::default(), async |ctx| {
            let mut handles = Vec::new();
            for i in 0..100 {
                handles.push(ctx.spawn(async move |ctx| {
                    ctx.sleep(Duration::from_millis(1 + i % 10)).await;
                    ctx.dispatch(1);
                }));
            }
            for handle in handles {
                handle.await;
            }
        })
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().0.len(), 100);
    }

    #[test]
    fn losing_sleep_leaves_no_timer_behind() {
        let (a, b) = pair();
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let winner = First(readable(ctx, &a), ctx.sleep(Duration::from_millis(50))).await;
            assert_eq!(winner, Winner::A);
            ctx.dispatch(1);
            pending::<()>().await;
        })
        .unwrap();
        assert!(has_timers(&event_loop));
        wake_by_writing(&event_loop, &b);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().0, vec![1]);
        assert!(!has_timers(&event_loop));
        assert!(
            !is_readable(&event_loop, 100),
            "the OS timer is still armed"
        );
    }

    #[test]
    fn winning_sleep_drops_the_other_future() {
        let (a, b) = pair();
        let a = Rc::new(a);
        let a_in_loop = a.clone();
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let readable = readable(ctx, &a_in_loop);
            let winner = First(readable, ctx.sleep(Duration::from_millis(10))).await;
            assert_eq!(winner, Winner::B);
            ctx.dispatch(1);
            pending::<()>().await;
        })
        .unwrap();
        while event_loop.state_mut().0.is_empty() {
            assert!(is_readable(&event_loop, 2000));
            assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        }
        rustix::io::write(&b, b"x").unwrap();
        assert!(!is_readable(&event_loop, 0));
        assert!(is_readable(&*a, 0));
    }

    #[test]
    fn drift_free_interval() {
        let period = Duration::from_millis(10);
        let start = Instant::now();
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let mut next = Instant::now();
            for i in 0..5 {
                next += period;
                ctx.sleep_until(next).await;
                ctx.dispatch(i);
            }
        })
        .unwrap();
        run_until_root_finishes(&mut event_loop);
        assert_eq!(event_loop.state_mut().0, vec![0, 1, 2, 3, 4]);
        assert!(start.elapsed() >= period * 5);
    }
}
