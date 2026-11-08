use crate::{
    WakeQueue,
    instant::Instant,
    ready::ReadyQueue,
    selectors::Direction,
    storage::Storage,
    test_utils::{counting_waker, flagged_task},
};
use core::{cell::Cell, mem::MaybeUninit};

type TestStorage<'a, S> = Storage<'a, S, 1, 64, 0, 2, 2, ()>;

#[test]
fn clearing_drops_live_wakers() {
    let drops = Cell::new(0);
    let mut place = MaybeUninit::uninit();
    let storage = TestStorage::init(
        &mut place,
        ReadyQueue::new({
            static QUEUE: WakeQueue<1> = WakeQueue::new();
            &QUEUE
        }),
        (),
        (),
    );
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
    let mut place = MaybeUninit::uninit();
    let storage = TestStorage::init(
        &mut place,
        ReadyQueue::new({
            static QUEUE: WakeQueue<1> = WakeQueue::new();
            &QUEUE
        }),
        (),
        (),
    );
    storage.tasks.insert(flagged_task(&dropped));
    storage.clear();
    assert!(dropped.get());
}
