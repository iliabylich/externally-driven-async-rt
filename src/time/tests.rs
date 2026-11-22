use core::{
    future::{Future, pending},
    pin::pin,
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    Selector,
    tasks::{RootStatus, WakeQueue},
    testing::{
        EventLoopTestingExt, FlagOnDrop, TestContext, TestLoop, TestStorage, assert_yields,
        sleep_blocking, wake_expired_timers,
    },
    time::{Instant, Timers, TimersFullError},
};

#[test]
fn future_finishing_first_wins() {
    let selector = Selector::new().unwrap();
    let (ctx, root): (TestContext<'_>, _) = {
        static STORAGE: TestStorage = TestStorage::new();
        STORAGE.claim(&selector).unwrap()
    };
    ctx.storage.tasks.spawn_root(root, ctx.claimed, async move {
        let result = ctx
            .timeout(Duration::from_secs(60), ctx.notified())
            .await
            .unwrap();
        ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
        pending::<()>().await;
    });
    assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
    assert!(ctx.storage.timers.next_deadline().is_some());
    ctx.storage.notification.fire();
    assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
    assert_yields!(ctx.storage.events.iter_removing(), [1]);
    assert_eq!(ctx.storage.timers.next_deadline(), None);
    ctx.clear();
}

#[test]
fn elapsing_drops_the_future() {
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let selector = Selector::new().unwrap();
    let (ctx, root): (TestContext<'_>, _) = {
        static STORAGE: TestStorage = TestStorage::new();
        STORAGE.claim(&selector).unwrap()
    };
    ctx.storage.tasks.spawn_root(root, ctx.claimed, async move {
        let result = ctx
            .timeout(Duration::from_millis(10), async {
                let _flag = FlagOnDrop::new(&DROPPED);
                pending::<()>().await;
            })
            .await
            .unwrap();
        ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
        pending::<()>().await;
    });
    assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
    assert!(!DROPPED.load(Relaxed));
    sleep_blocking(Duration::from_millis(10));
    wake_expired_timers(&ctx.storage.timers);
    assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
    assert_yields!(ctx.storage.events.iter_removing(), [0]);
    assert!(DROPPED.load(Relaxed));
    assert_eq!(ctx.storage.timers.next_deadline(), None);
    ctx.clear();
}

#[test]
fn ready_future_beats_a_zero_timeout() {
    static STORAGE: TestStorage = TestStorage::new();
    let selector = Selector::new().unwrap();
    let ctx: TestContext<'_> = STORAGE.claim(&selector).unwrap().0;
    let mut cx = Context::from_waker(Waker::noop());
    let mut timeout = pin!(ctx.timeout(Duration::ZERO, async {
        let value = 1;
        let borrowed = &value;
        core::future::ready(()).await;
        *borrowed
    }));
    assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Ready(Ok(Some(1))));
    assert_eq!(ctx.storage.timers.next_deadline(), None);
}

#[test]
fn pending_pinned_future_keeps_its_timer_until_dropped() {
    static STORAGE: TestStorage = TestStorage::new();
    let selector = Selector::new().unwrap();
    let ctx: TestContext<'_> = STORAGE.claim(&selector).unwrap().0;
    let mut cx = Context::from_waker(Waker::noop());
    {
        let mut timeout = pin!(ctx.timeout(Duration::from_secs(60), async {
            let value = 1;
            let borrowed = &value;
            pending::<()>().await;
            *borrowed
        }));
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
        assert!(ctx.storage.timers.next_deadline().is_some());
    }
    assert_eq!(ctx.storage.timers.next_deadline(), None);
}

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
    assert_eq!(
        pin!(timers.sleep_until(Instant::now())).poll(&mut cx),
        Poll::Ready(Ok(()))
    );
    assert_eq!(timers.next_deadline(), None);
}

#[test]
fn dropping_a_pending_sleep_releases_its_timer() {
    let timers = &Timers::<1>::new();
    let mut cx = Context::from_waker(Waker::noop());
    let deadline = Instant::now().saturating_add(Duration::from_secs(60));
    {
        let mut sleep = pin!(timers.sleep_until(deadline));
        assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(timers.next_deadline(), Some(deadline));
    }
    assert_eq!(timers.next_deadline(), None);
}

#[test]
fn finished_sleep_releases_its_timer() {
    let timers = &Timers::<1>::new();
    let mut cx = Context::from_waker(Waker::noop());
    let mut sleep =
        pin!(timers.sleep_until(Instant::now().saturating_add(Duration::from_millis(10))));
    assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Pending);
    sleep_blocking(Duration::from_millis(10));
    assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    assert_eq!(timers.next_deadline(), None);
}

#[test]
fn sleep_without_a_free_timer_fails() {
    let timers = &Timers::<1>::new();
    let mut cx = Context::from_waker(Waker::noop());
    let deadline = Instant::now().saturating_add(Duration::from_secs(60));
    let mut first = pin!(timers.sleep_until(deadline));
    let mut second = pin!(timers.sleep_until(deadline));
    assert_eq!(first.as_mut().poll(&mut cx), Poll::Pending);
    assert_eq!(
        second.as_mut().poll(&mut cx),
        Poll::Ready(Err(TimersFullError))
    );
}

#[test]
fn repolling_a_pending_sleep_replaces_its_waker() {
    static WAKERS: WakeQueue<2> = WakeQueue::new();
    let timers = &Timers::<1>::new();
    let deadline = Instant::now().saturating_add(Duration::from_secs(60));
    let mut sleep = pin!(timers.sleep_until(deadline));
    for idx in [0, 1] {
        let waker = WAKERS.waker(idx);
        let mut cx = Context::from_waker(&waker);
        assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Pending);
    }
    let woken = timers.take_expired(deadline).unwrap();
    assert_eq!(WAKERS.idx_by_waker_data(&woken), 1);
}
