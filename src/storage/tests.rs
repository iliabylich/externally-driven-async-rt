use crate::{
    instant::Instant,
    selectors::Direction,
    storage::Storage,
    test_utils::{DropFlag, counting_waker, flagged_task},
};
use core::{cell::Cell, mem::MaybeUninit};

type TestStorage<'a, S> = Storage<'a, S, 1, 64, 0, 2, 2, ()>;

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
    let dropped = Cell::new(false);
    let mut place = MaybeUninit::uninit();
    let storage = TestStorage::init(&mut place, (), ());
    storage.tasks.insert(flagged_task(&dropped));
    // SAFETY: destroyed once, no task is being polled, and the storage isn't used afterwards.
    unsafe { storage.destroy() };
    assert!(dropped.get());
}

#[test]
fn state_is_usable_and_dropped_by_destroy() {
    struct State<'a> {
        value: u32,
        _flag: DropFlag<'a>,
    }
    let dropped = Cell::new(false);
    let state = State {
        value: 7,
        _flag: DropFlag(&dropped),
    };
    let mut place = MaybeUninit::uninit();
    let storage = TestStorage::init(&mut place, (), state);
    // SAFETY: nothing else references the state.
    let state = unsafe { &mut *storage.state.get() };
    assert_eq!(state.value, 7);
    state.value = 8;
    // SAFETY: as above.
    assert_eq!(unsafe { &*storage.state.get() }.value, 8);
    assert!(!dropped.get());
    // SAFETY: destroyed once, no task is being polled, and the storage isn't used afterwards.
    unsafe { storage.destroy() };
    assert!(dropped.get());
}
