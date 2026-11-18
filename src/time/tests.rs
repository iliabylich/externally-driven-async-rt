use core::{
    future::{Future, pending},
    pin::pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    Selector,
    testing::{
        EventLoopTestingExt, TestContext, TestLoop, TestStorage, assert_yields, socketpair,
        wait_readable,
    },
};

#[test]
#[cfg_attr(miri, ignore)]
fn future_finishing_first_wins() {
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let result = ctx
                .timeout(Duration::from_secs(60), wait_readable(&ctx, &rx))
                .await
                .unwrap();
            ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();
    assert!(event_loop.has_timers());
    assert_eq!(event_loop.registrations(), 1);
    event_loop.wake_by_writing(&tx);
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), [1]);
    assert!(!event_loop.has_timers());
    assert_eq!(event_loop.registrations(), 0);
}

#[test]
#[cfg_attr(miri, ignore)]
fn elapsing_drops_the_future() {
    let (rx, _tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let result = ctx
                .timeout(Duration::from_millis(10), wait_readable(&ctx, &rx))
                .await
                .unwrap();
            ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(event_loop.run_until_events(), [0]);
    assert!(!event_loop.has_timers());
    assert_eq!(event_loop.registrations(), 0);
}

#[test]
#[cfg_attr(miri, ignore)]
fn ready_future_beats_a_zero_timeout() {
    let (rx, tx) = socketpair();
    rustix::io::write(&tx, b"x").unwrap();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let result = ctx
                .timeout(Duration::ZERO, wait_readable(&ctx, &rx))
                .await
                .unwrap();
            ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_yields!(event_loop.events(), [1]);
    assert!(!event_loop.has_timers());
}

#[test]
fn ready_pinned_future_resolves_and_releases_the_timer() {
    static STORAGE: TestStorage = TestStorage::new();
    let selector = Selector::new().unwrap();
    let ctx: TestContext<'_> = STORAGE.claim(&selector).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut timeout = pin!(ctx.timeout(Duration::from_secs(60), async {
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
    let ctx: TestContext<'_> = STORAGE.claim(&selector).unwrap();
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
