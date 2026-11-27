use core::{future::pending, time::Duration};

use rustix::fd::{AsFd, AsRawFd};

use crate::{EventLoop, LoopStorage, Selector, testing::assert_yields, time::Instant};

use super::{EventLoopExt, FdExt};

type Storage = LoopStorage<u32, 5, 1, 0, 256, 1, 1>;

#[test]
fn idle_loop_is_not_readable_and_drains_to_nothing() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop =
        EventLoop::new(&STORAGE, &mut selector, async |_| pending().await).unwrap();
    assert!(!event_loop.is_readable_now());
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), []);
}

#[test]
fn loop_exposes_the_selector_fd() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let fd = selector.as_fd().as_raw_fd();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |_| pending().await).unwrap();
    assert_eq!(event_loop.as_fd().as_raw_fd(), fd);
}


#[test]
fn drift_free_interval_from_relative_sleeps() {
    static STORAGE: Storage = Storage::new();
    let period = Duration::from_millis(10);
    let start = Instant::now();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        let mut next = Instant::now();
        for i in 0..5 {
            next = next.saturating_add(period);
            ctx.sleep(next.saturating_sub(Instant::now()))
                .await
                .unwrap();
            ctx.dispatch(i).unwrap();
        }
    })
    .unwrap();
    event_loop.run_until_root_finishes();
    assert_yields!(event_loop.events(), [0, 1, 2, 3, 4]);
    assert!(Instant::now().saturating_sub(start) >= period * 5);
}

#[test]
fn spin_blocks_until_a_sleep_finishes() {
    static STORAGE: Storage = Storage::new();
    let start = Instant::now();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        ctx.sleep(Duration::from_millis(20)).await.unwrap();
    })
    .unwrap();
    assert_eq!(event_loop.spin(), Ok(()));
    assert!(Instant::now().saturating_sub(start) >= Duration::from_millis(20));
}

#[test]
fn spin_drops_events_after_every_drain() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        for i in 0..10 {
            ctx.sleep(Duration::from_millis(1)).await.unwrap();
            ctx.dispatch(i).unwrap();
        }
    })
    .unwrap();
    assert_eq!(event_loop.spin(), Ok(()));
}
