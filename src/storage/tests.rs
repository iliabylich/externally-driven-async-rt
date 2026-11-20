use core::cell::Cell;

use crate::{LoopStorage, Selector, selector::Direction, testing::counting_waker, time::Instant};

type SmallStorage = LoopStorage<(), 1, 1, 64, 0, 2, 2>;

#[test]
fn clearing_drops_live_wakers() {
    let drops = Cell::new(0);
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: SmallStorage = SmallStorage::new();
        STORAGE.claim(&selector).unwrap().0
    };
    let timer_waker = counting_waker(&drops);
    let _ = ctx.storage.timers.alloc(Instant::now(), &timer_waker);
    drop(timer_waker);
    let io = ctx.storage.io_waiters.register(3, Direction::Read).unwrap();
    let io_waker = counting_waker(&drops);
    io.waker(Direction::Read).store(&io_waker);
    drop(io_waker);
    assert_eq!(drops.get(), 2);
    ctx.clear();
    assert_eq!(drops.get(), 4);
}

#[test]
fn a_storage_can_only_be_claimed_once() {
    static STORAGE: SmallStorage = SmallStorage::new();
    let selector = Selector::new().unwrap();
    assert!(STORAGE.claim(&selector).is_some());
    assert!(STORAGE.claim(&selector).is_none());
}

#[test]
fn storage_can_be_built_at_runtime() {
    let _storage = SmallStorage::new();
}
