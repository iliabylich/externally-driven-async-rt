use crate::{
    instant::Instant,
    selectors::Direction,
    storage::Storage,
    test_utils::{DropFlag, counting_waker, flagged_task},
};
use core::{cell::Cell, mem::MaybeUninit};

type TestStorage<S> = Storage<S, 1, 64, 0, 2, 2, ()>;

#[test]
fn destroying_drops_live_wakers() {
    let drops = Cell::new(0);
    let mut place = MaybeUninit::uninit();
    let storage = TestStorage::init(&mut place, (), ());
    storage.timers.alloc(Instant::now(), counting_waker(&drops));
    let io = storage.io_waiters.register(3, Direction::Read);
    let io_waker = counting_waker(&drops);
    storage.io_waiters.set_waker(io, &io_waker);
    drop(io_waker);
    assert_eq!(drops.get(), 1);
    // SAFETY: destroyed once, no task is being polled, and the storage isn't used afterwards.
    unsafe { storage.destroy() };
    assert_eq!(drops.get(), 3);
}

#[test]
fn destroying_drops_live_tasks() {
    static mut DROPPED: bool = false;
    let mut place = MaybeUninit::uninit();
    let storage = TestStorage::init(&mut place, (), ());
    storage.tasks.insert(flagged_task(&raw mut DROPPED));
    // SAFETY: destroyed once, no task is being polled, and the storage isn't used afterwards.
    unsafe { storage.destroy() };
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(unsafe { DROPPED });
}

#[test]
fn state_is_usable_and_dropped_by_destroy() {
    struct State {
        value: u32,
        _flag: DropFlag,
    }
    static mut DROPPED: bool = false;
    let state = State {
        value: 7,
        _flag: DropFlag(&raw mut DROPPED),
    };
    let mut place = MaybeUninit::uninit();
    let storage = TestStorage::init(&mut place, (), state);
    // SAFETY: nothing else references the state.
    let state = unsafe { &mut *storage.state.get() };
    assert_eq!(state.value, 7);
    state.value = 8;
    // SAFETY: as above.
    assert_eq!(unsafe { &*storage.state.get() }.value, 8);
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(!unsafe { DROPPED });
    // SAFETY: destroyed once, no task is being polled, and the storage isn't used afterwards.
    unsafe { storage.destroy() };
    // SAFETY: as above.
    assert!(unsafe { DROPPED });
}
