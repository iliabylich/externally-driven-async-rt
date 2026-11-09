use crate::{
    LoopStorage,
    instant::Instant,
    selectors::Direction,
    test_utils::{counting_waker, flagged_task},
};
use core::cell::Cell;

type SmallStorage = LoopStorage<(), 1, 64, 0, 2, 2, ()>;

#[test]
fn clearing_drops_live_wakers() {
    let drops = Cell::new(0);
    let storage = {
        static STORAGE: SmallStorage = SmallStorage::new();
        STORAGE.claim((), ())
    };
    storage.timers.alloc(Instant::now(), counting_waker(&drops));
    let io = storage.io_waiters.register(3, Direction::Read);
    let io_waker = counting_waker(&drops);
    storage.io_waiters.set_waker(io, &io_waker);
    drop(io_waker);
    assert_eq!(drops.get(), 1);
    storage.clear();
    assert_eq!(drops.get(), 3);
}

#[test]
fn clearing_drops_live_tasks() {
    let dropped = Cell::new(false);
    let storage = {
        static STORAGE: SmallStorage = SmallStorage::new();
        STORAGE.claim((), ())
    };
    storage.insert(flagged_task(&dropped));
    storage.clear();
    assert!(dropped.get());
}

#[test]
#[should_panic(expected = "a WakeQueue can only be used by one EventLoop")]
fn a_storage_can_only_be_claimed_once() {
    static STORAGE: SmallStorage = SmallStorage::new();
    STORAGE.claim((), ());
    STORAGE.claim((), ());
}
