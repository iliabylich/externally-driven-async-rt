use core::{
    cell::Cell,
    future::Future,
    pin::pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{LoopStorage, Selector, testing::counting_waker, time::Instant};

type Storage = LoopStorage<(), 1, 1, 0, 0, 1, 1>;

#[test]
fn clearing_drops_live_wakers() {
    static STORAGE: Storage = Storage::new();
    let drops = Cell::new(0);
    let selector = Selector::new().unwrap();
    let ctx = STORAGE.claim(&selector).unwrap().0;
    let timer_waker = counting_waker(&drops);
    let mut sleep = pin!(
        ctx.storage
            .timers
            .sleep_until(Instant::now().saturating_add(Duration::from_secs(60)))
    );
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(&timer_waker)),
        Poll::Pending
    );
    drop(timer_waker);
    assert_eq!(drops.get(), 1);
    ctx.clear();
    assert_eq!(drops.get(), 2);
}

#[test]
fn a_storage_can_only_be_claimed_once() {
    static STORAGE: Storage = Storage::new();
    let selector = Selector::new().unwrap();
    assert!(STORAGE.claim(&selector).is_some());
    assert!(STORAGE.claim(&selector).is_none());
}

#[test]
fn storage_can_be_built_at_runtime() {
    let _storage = Storage::new();
}
