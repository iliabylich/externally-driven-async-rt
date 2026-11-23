use core::{
    cell::Cell,
    future::{Future, pending},
    pin::pin,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    Selector,
    notify::Notification,
    testing::{
        EventLoopTestingExt, FdTestingExt, TestContext, TestLoop, TestStorage, assert_yields,
        counting_waker, fake_instant, wake_expired_timers,
    },
    time::Instant,
};

#[test]
#[cfg_attr(miri, ignore)]
fn notify_makes_the_loop_readable_and_wakes_the_waiter() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            for i in 1..=u32::MAX {
                ctx.notified().await;
                ctx.dispatch(i).unwrap();
            }
        },
    )
    .unwrap();
    assert!(!event_loop.is_readable_now());

    event_loop.notify().unwrap();
    assert_eq!(event_loop.run_until_events(), [1]);
    assert!(!event_loop.is_readable_now());

    event_loop.notify().unwrap();
    assert_eq!(event_loop.run_until_events(), [2]);
    assert!(!event_loop.is_readable_now());
}

#[test]
#[cfg_attr(miri, ignore)]
fn notifications_coalesce() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            for i in 1..=u32::MAX {
                ctx.notified().await;
                ctx.dispatch(i).unwrap();
            }
        },
    )
    .unwrap();

    event_loop.notify().unwrap();
    event_loop.notify().unwrap();
    event_loop.notify().unwrap();
    assert_eq!(event_loop.run_until_events(), [1]);
    assert!(!event_loop.is_readable_now());
}

#[test]
fn notification_without_a_waiter_is_kept() {
    static NOW: AtomicU64 = AtomicU64::new(0);
    fn now() -> Instant {
        fake_instant(&NOW)
    }
    let selector = Selector::new().unwrap();
    let (ctx, root): (TestContext<'_>, _) = {
        static STORAGE: TestStorage = TestStorage::with_clock(now);
        STORAGE.claim(&selector).unwrap()
    };
    ctx.storage.tasks.spawn_root(root, ctx.claimed, async move {
        ctx.sleep(Duration::from_millis(10)).await.unwrap();
        ctx.dispatch(1).unwrap();
        ctx.notified().await;
        ctx.dispatch(2).unwrap();
        pending::<()>().await;
    });
    assert_eq!(ctx.storage.tasks.run_ready(), Poll::Pending);

    ctx.storage.notification.fire();
    assert_eq!(ctx.storage.tasks.run_ready(), Poll::Pending);
    assert_yields!(ctx.storage.events.iter_removing(), []);

    NOW.store(10, Relaxed);
    wake_expired_timers(&ctx.storage.timers);
    assert_eq!(ctx.storage.tasks.run_ready(), Poll::Pending);
    assert_yields!(ctx.storage.events.iter_removing(), [1, 2]);
    ctx.clear();
}

#[test]
fn fire_before_poll_resolves_immediately() {
    let notification = Notification::new();
    let mut cx = Context::from_waker(Waker::noop());
    notification.fire();
    assert_eq!(pin!(notification.notified()).poll(&mut cx), Poll::Ready(()));
    assert_eq!(pin!(notification.notified()).poll(&mut cx), Poll::Pending);
}

#[test]
fn dropping_a_pending_notified_forgets_its_waker() {
    let drops = Cell::new(0);
    let notification = Notification::new();
    {
        let waker = counting_waker(&drops);
        let mut cx = Context::from_waker(&waker);
        let mut notified = pin!(notification.notified());
        assert_eq!(notified.as_mut().poll(&mut cx), Poll::Pending);
    }
    assert_eq!(drops.get(), 2);
}

#[test]
fn clear_drops_a_pending_notification() {
    let notification = Notification::new();
    notification.fire();
    notification.clear();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(pin!(notification.notified()).poll(&mut cx), Poll::Pending);
}
