use core::{
    cell::Cell,
    future::{Future, pending},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    Selector,
    notify::{Notification, Notified},
    testing::{
        EventLoopTestingExt, FdTestingExt, TestLoop, TestStorage, assert_yields, counting_waker,
    },
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
            for i in 1.. {
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
            for i in 1.. {
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
#[cfg_attr(miri, ignore)]
fn notification_without_a_waiter_is_kept() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            ctx.sleep(Duration::from_millis(10)).await;
            ctx.dispatch(1).unwrap();
            ctx.notified().await;
            ctx.dispatch(2).unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();

    event_loop.notify().unwrap();
    assert!(event_loop.is_readable_now());
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), []);

    assert_eq!(event_loop.run_until_events(), [1, 2]);
}

#[test]
fn fire_before_poll_resolves_immediately() {
    let notification = Notification::new();
    let mut cx = Context::from_waker(Waker::noop());
    notification.fire();
    let mut notified = Notified::new(&notification);
    assert_eq!(Pin::new(&mut notified).poll(&mut cx), Poll::Ready(()));
    let mut notified = Notified::new(&notification);
    assert_eq!(Pin::new(&mut notified).poll(&mut cx), Poll::Pending);
}

#[test]
fn dropping_a_pending_notified_forgets_its_waker() {
    let drops = Cell::new(0);
    let notification = Notification::new();
    {
        let waker = counting_waker(&drops);
        let mut cx = Context::from_waker(&waker);
        let mut notified = Notified::new(&notification);
        assert_eq!(Pin::new(&mut notified).poll(&mut cx), Poll::Pending);
    }
    assert_eq!(drops.get(), 2);
}

#[test]
fn clear_drops_a_pending_notification() {
    let notification = Notification::new();
    notification.fire();
    notification.clear();
    let mut cx = Context::from_waker(Waker::noop());
    let mut notified = Notified::new(&notification);
    assert_eq!(Pin::new(&mut notified).poll(&mut cx), Poll::Pending);
}
