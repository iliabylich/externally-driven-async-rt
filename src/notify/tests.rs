use core::{
    cell::{Cell, RefCell},
    future::{Future, pending},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    Selector,
    notify::{Notification, Notified},
    testing::{EventLoopTestingExt, Log, TestLoop, TestStorage, counting_waker, is_readable},
};

#[test]
#[cfg_attr(miri, ignore)]
fn notify_makes_the_loop_readable_and_wakes_the_waiter() {
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
            for i in 1.. {
                ctx.notified().await;
                ctx.dispatch(i);
            }
        },
    )
    .unwrap();
    assert!(!is_readable(&event_loop, 0));

    event_loop.notify().unwrap();
    event_loop.run_until_events(1);
    assert_eq!(event_loop.state_mut().events(), [1]);
    assert!(!is_readable(&event_loop, 0));

    event_loop.notify().unwrap();
    event_loop.run_until_events(2);
    assert_eq!(event_loop.state_mut().events(), [1, 2]);
    assert!(!is_readable(&event_loop, 0));
}

#[test]
#[cfg_attr(miri, ignore)]
fn notifications_coalesce() {
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
            for i in 1.. {
                ctx.notified().await;
                ctx.dispatch(i);
            }
        },
    )
    .unwrap();

    event_loop.notify().unwrap();
    event_loop.notify().unwrap();
    event_loop.notify().unwrap();
    event_loop.run_until_events(1);
    assert!(!is_readable(&event_loop, 0));
    assert_eq!(event_loop.state_mut().events(), [1]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn notification_without_a_waiter_is_kept() {
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
            ctx.sleep(Duration::from_millis(10)).await;
            ctx.dispatch(1);
            ctx.notified().await;
            ctx.dispatch(2);
            pending::<()>().await;
        },
    )
    .unwrap();

    event_loop.notify().unwrap();
    assert!(is_readable(&event_loop, 0));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert!(event_loop.state_mut().events().is_empty());

    event_loop.run_until_events(2);
    assert_eq!(event_loop.state_mut().events(), [1, 2]);
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
