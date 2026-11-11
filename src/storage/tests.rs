use core::cell::{Cell, RefCell};

use crate::{LoopStorage, selector::Direction, testing::counting_waker, time::Instant};

type SmallStorage = LoopStorage<1, 64, 0, 2, 2>;

#[test]
fn clearing_drops_live_wakers() {
    let drops = Cell::new(0);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: SmallStorage = SmallStorage::new();
        STORAGE.claim(&(), &state)
    };
    storage
        .timers()
        .alloc(Instant::now(), counting_waker(&drops));
    let (_, io) = storage
        .io_waiters()
        .register(&Cell::new(None), 3, Direction::Read);
    let io_waker = counting_waker(&drops);
    io.waker(Direction::Read).store(&io_waker);
    drop(io_waker);
    assert_eq!(drops.get(), 1);
    storage.clear();
    assert_eq!(drops.get(), 3);
}

#[test]
#[should_panic(expected = "a WakeQueue can only be used by one EventLoop")]
fn a_storage_can_only_be_claimed_once() {
    static STORAGE: SmallStorage = SmallStorage::new();
    let state = RefCell::new(());
    STORAGE.claim(&(), &state);
    STORAGE.claim(&(), &state);
}

#[test]
fn storage_can_be_built_at_runtime() {
    let _storage = SmallStorage::new();
}
