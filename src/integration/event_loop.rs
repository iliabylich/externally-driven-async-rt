use core::{future::pending, time::Duration};

use rustix::fd::{AsFd, AsRawFd};

use crate::{
    Selector,
    testing::{TestLoop, TestStorage, assert_yields},
    time::Instant,
};

use super::{EventLoopExt, FdExt};

#[test]
fn idle_loop_is_not_readable_and_drains_to_nothing() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |_| pending().await,
    )
    .unwrap();
    assert!(!event_loop.is_readable_now());
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), []);
}

#[test]
fn loop_exposes_the_selector_fd() {
    let mut selector = Selector::new().unwrap();
    let fd = selector.as_fd().as_raw_fd();
    let event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |_| pending().await,
    )
    .unwrap();
    assert_eq!(event_loop.as_fd().as_raw_fd(), fd);
}

#[test]
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
    assert!(Instant::now().saturating_sub(start) >= Duration::from_millis(20));
    assert_yields!(event_loop.events(), [1]);
}

#[test]
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
                ctx.sleep(next.saturating_sub(Instant::now()))
                    .await
                    .unwrap();
                ctx.dispatch(i).unwrap();
            }
        },
    )
    .unwrap();
    event_loop.run_until_root_finishes();
    assert_yields!(event_loop.events(), [0, 1, 2, 3, 4]);
    assert!(Instant::now().saturating_sub(start) >= period * 5);
}
