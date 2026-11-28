use core::{
    cell::Cell,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use crate::{
    notify::Notification,
    testing::{counting_waker, flag_waker},
};

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
fn fire_wakes_a_pending_waiter() {
    let woken = Cell::new(false);
    let notification = Notification::new();
    let waker = flag_waker(&woken);
    let mut cx = Context::from_waker(&waker);
    let mut notified = pin!(notification.notified());
    assert_eq!(notified.as_mut().poll(&mut cx), Poll::Pending);
    assert!(!woken.get());
    notification.fire();
    assert!(woken.get());
    assert_eq!(notified.as_mut().poll(&mut cx), Poll::Ready(()));
}

#[test]
fn fires_coalesce() {
    let notification = Notification::new();
    let mut cx = Context::from_waker(Waker::noop());
    notification.fire();
    notification.fire();
    notification.fire();
    assert_eq!(pin!(notification.notified()).poll(&mut cx), Poll::Ready(()));
    assert_eq!(pin!(notification.notified()).poll(&mut cx), Poll::Pending);
}
