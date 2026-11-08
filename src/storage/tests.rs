use crate::{
    Capacities,
    instant::Instant,
    selectors::{Direction, IoAction, IoChange, Token},
    storage::{IoIndex, Storage, StorageLayout, TaskId},
    task::JoinStatus,
    test_utils::{TEST_CAPACITIES, TestStorage, assert_yields, stack_memory},
};
use core::{
    alloc::Layout,
    cell::Cell,
    mem::MaybeUninit,
    ptr::NonNull,
    task::{RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

// Does nothing; told apart by its data pointer, `id` (`Waker::data`).
fn waker(id: usize) -> Waker {
    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(|data| RawWaker::new(data, &VTABLE), |_| {}, |_| {}, |_| {});
    // SAFETY: the vtable never touches the data pointer, and its functions do nothing.
    unsafe { Waker::new(core::ptr::without_provenance(id), &VTABLE) }
}

// Does nothing but count its drops (clones included) in `drops`.
fn counting_waker(drops: &Cell<u32>) -> Waker {
    static VTABLE: RawWakerVTable = RawWakerVTable::new(
        |data| RawWaker::new(data, &VTABLE),
        |_| {},
        |_| {},
        |data| {
            // SAFETY: the data is the `Cell` from `counting_waker`, which every test keeps
            // alive for as long as its wakers.
            let drops = unsafe { &*data.cast::<Cell<u32>>() };
            drops.set(drops.get() + 1);
        },
    );
    // SAFETY: the vtable only reads the data as a `&Cell<u32>`, and `drops` outlives the
    // waker (see above).
    unsafe { Waker::new(core::ptr::from_ref(drops).cast(), &VTABLE) }
}

#[track_caller]
fn assert_expires(storage: &Storage<(), ()>, now: Instant, ids: impl IntoIterator<Item = usize>) {
    let expired = core::iter::from_fn(|| storage.take_expired_timer(now));
    assert_yields(expired.map(|waker| waker.data().addr()), ids);
}

#[track_caller]
fn assert_pending_changes(storage: &Storage<(), ()>, expected: impl IntoIterator<Item = IoChange>) {
    let pending = storage
        .io_waiters
        .iter()
        .filter_map(|(index, io)| io.pending_change(index));
    assert_yields(pending, expected);
}

fn noop_waker() -> Waker {
    Waker::noop().clone()
}

#[test]
fn alloc_get_release() {
    let now = Instant::now();
    let drops = Cell::new(0);
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 2, 1);
    let storage = backing.storage();
    let index = storage.alloc_timer(now, counting_waker(&drops));
    assert_eq!(storage.timers.get(index).unwrap().deadline.get(), now);

    storage.release_timer(index);
    assert_eq!(drops.get(), 1);
    assert!(storage.timers.get(index).is_none());
    storage.release_timer(index);
    assert_eq!(drops.get(), 1);
}

#[test]
#[should_panic(expected = "more than 1 concurrent sleeps")]
fn timer_capacity_is_bounded() {
    let now = Instant::now();
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let storage = backing.storage();
    storage.alloc_timer(now, noop_waker());
    storage.alloc_timer(now, noop_waker());
}

#[test]
fn expire_wakes_due_timers_in_deadline_order() {
    let earliest = Instant::now();
    let now = earliest + Duration::from_millis(1);
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 8, 1);
    let storage = backing.storage();
    storage.alloc_timer(now + Duration::from_millis(5), waker(3));
    storage.alloc_timer(now, waker(2));
    storage.alloc_timer(now + Duration::from_millis(5), waker(4));
    storage.alloc_timer(now + Duration::from_millis(10), waker(5));
    storage.alloc_timer(earliest, waker(1));

    assert_expires(storage, now + Duration::from_millis(5), [1, 2, 3, 4]);
    assert_expires(storage, now + Duration::from_millis(9), []);
    assert_eq!(
        storage.next_timer_deadline(),
        Some(now + Duration::from_millis(10))
    );
}

#[test]
fn equal_deadlines_expire_in_insertion_order_across_slot_reuse() {
    let now = Instant::now();
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 2, 1);
    let storage = backing.storage();
    let first = storage.alloc_timer(now, noop_waker());
    storage.alloc_timer(now, waker(1));
    storage.release_timer(first);
    storage.alloc_timer(now, waker(2));
    assert_expires(storage, now, [1, 2]);
}

#[test]
fn released_timer_is_not_woken() {
    let now = Instant::now();
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let storage = backing.storage();
    let index = storage.alloc_timer(now, waker(1));
    storage.release_timer(index);
    assert_expires(storage, now, []);
    assert_eq!(storage.next_timer_deadline(), None);
}

#[test]
fn replaced_waker_is_the_one_woken() {
    let now = Instant::now();
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let storage = backing.storage();
    let index = storage.alloc_timer(now, waker(1));
    storage.set_timer_waker(index, &waker(2));
    assert_expires(storage, now, [2]);
}

#[test]
fn io_alloc_get_release() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let storage = backing.storage();
    let index = storage.alloc_io(3, Direction::Write);
    let io = storage.io_waiters.get(index).unwrap();
    assert_eq!(io.direction.get(), Direction::Write);
    storage.set_io_waker(index, &noop_waker());
    assert!(storage.has_io_waker(index));
    storage.io_waiters.free(index);
    assert!(storage.io_waiters.get(index).is_none());
    assert!(!storage.has_io_waker(index));
    let reused = storage.alloc_io(3, Direction::Read);
    assert_ne!(reused, index);
}

#[test]
fn io_changes_follow_the_stored_waker() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 2);
    let storage = backing.storage();
    let reader = storage.alloc_io(3, Direction::Read);
    let writer = storage.alloc_io(4, Direction::Write);
    assert_pending_changes(storage, []);

    let change = |action| IoChange {
        token: Token(reader.to_bits()),
        fd: 3,
        direction: Direction::Read,
        action,
    };
    storage.set_io_waker(reader, &noop_waker());
    assert_pending_changes(storage, [change(IoAction::Arm)]);
    storage.io_waiters.get(reader).unwrap().armed.set(true);
    assert_pending_changes(storage, []);

    storage.remove_io_waker(reader);
    assert_pending_changes(storage, [change(IoAction::Disarm)]);
    assert_eq!(IoIndex::from_bits(reader.to_bits()), reader);
    assert!(storage.io_waiters.get(writer).is_some());
}

#[test]
#[should_panic(expected = "more than 1 concurrent IO operations")]
fn io_capacity_is_bounded() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let storage = backing.storage();
    storage.alloc_io(3, Direction::Read);
    storage.alloc_io(3, Direction::Write);
}

#[test]
fn dropping_the_storage_drops_live_wakers() {
    let drops = Cell::new(0);
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 2, 2);
    let storage = backing.storage();
    storage.alloc_timer(Instant::now(), counting_waker(&drops));
    let io = storage.alloc_io(3, Direction::Read);
    let io_waker = counting_waker(&drops);
    storage.set_io_waker(io, &io_waker);
    drop(io_waker);
    assert_eq!(drops.get(), 1);
    drop(backing);
    assert_eq!(drops.get(), 3);
}

#[test]
#[should_panic(expected = "object pool capacity must not be zero")]
fn zero_capacity_is_rejected() {
    let mut memory = stack_memory();
    TestStorage::new(&mut memory, 1, 1, 0);
}

#[test]
fn next_timer_deadline_is_the_earliest_one() {
    let now = Instant::now();
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 4, 1);
    let storage = backing.storage();
    assert_eq!(storage.next_timer_deadline(), None);
    let later = now + Duration::from_millis(30);
    let earlier = now + Duration::from_millis(10);
    storage.alloc_timer(later, noop_waker());
    let earliest = storage.alloc_timer(earlier, noop_waker());
    storage.alloc_timer(now + Duration::from_millis(20), noop_waker());
    assert_eq!(storage.next_timer_deadline(), Some(earlier));
    storage.release_timer(earliest);
    assert_eq!(
        storage.next_timer_deadline(),
        Some(now + Duration::from_millis(20))
    );
}

struct DropFlag(*mut bool);

impl Drop for DropFlag {
    fn drop(&mut self) {
        // SAFETY: points to a `static mut` of the running test, which only that test's thread
        // uses, and no reference to it exists.
        unsafe { *self.0 = true };
    }
}

fn flagged_task(dropped: *mut bool) -> impl Future<Output = ()> {
    let flag = DropFlag(dropped);
    async move {
        let _flag = flag;
    }
}

#[test]
fn removing_a_task_drops_its_future_and_returns_its_join_waiter() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 2, 1, 1);
    let storage = backing.storage();
    static mut DROPPED: bool = false;
    let id = storage
        .insert_task(flagged_task(&raw mut DROPPED))
        .ok()
        .unwrap();
    assert!(storage.task(id).is_some());
    assert_eq!(
        storage.set_join_waiter(id, Waker::noop()),
        JoinStatus::Running
    );
    let join_waiter = storage.remove_task(id).unwrap();
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(unsafe { DROPPED });
    assert!(join_waiter.is_some());
    assert!(storage.task(id).is_none());
    assert_eq!(
        storage.set_join_waiter(id, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
fn finished_task_id_never_matches_a_reused_slot() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let storage = backing.storage();
    let first = storage.insert_task(async {}).ok().unwrap();
    storage.remove_task(first);
    let second = storage.insert_task(async {}).ok().unwrap();
    assert_ne!(first, second);
    assert!(storage.task(first).is_none());
    assert_eq!(
        storage.set_join_waiter(first, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
fn full_task_pool_hands_the_future_back() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let storage = backing.storage();
    storage.insert_task(async {}).ok().unwrap();
    static mut DROPPED: bool = false;
    let rejected = storage
        .insert_task(flagged_task(&raw mut DROPPED))
        .err()
        .unwrap();
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(!unsafe { DROPPED });
    drop(rejected);
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(unsafe { DROPPED });
    assert_eq!(storage.task_capacity(), 1);
}

#[test]
fn drop_all_tasks_and_drop_free_the_futures() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 2, 1, 1);
    let storage = backing.storage();
    static mut DROPPED: bool = false;
    static mut LEFT_OVER: bool = false;
    storage
        .insert_task(flagged_task(&raw mut DROPPED))
        .ok()
        .unwrap();
    storage.drop_all_tasks();
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(unsafe { DROPPED });
    assert!(storage.tasks.iter().next().is_none());

    storage
        .insert_task(flagged_task(&raw mut LEFT_OVER))
        .ok()
        .unwrap();
    drop(backing);
    // SAFETY: only this test's thread uses `LEFT_OVER`, and no reference to it exists.
    assert!(unsafe { LEFT_OVER });
}

struct SpawnOnDrop {
    storage: &'static Storage<(), ()>,
    // Set once the task was inserted.
    id: *mut Option<TaskId>,
    spawned: *mut Option<TaskId>,
}

impl Drop for SpawnOnDrop {
    fn drop(&mut self) {
        // SAFETY: points to a `static mut` of the running test, which only that test's thread
        // uses, and no reference to it exists.
        let id = unsafe { *self.id }.unwrap();
        assert!(self.storage.task(id).is_none());
        assert_eq!(
            self.storage.set_join_waiter(id, Waker::noop()),
            JoinStatus::Finished
        );
        let spawned = self.storage.insert_task(async {}).ok().unwrap();
        // SAFETY: as above.
        unsafe { *self.spawned = Some(spawned) };
    }
}

#[test]
fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 2, 1, 1);
    let storage = backing.storage();
    static mut ID: Option<TaskId> = None;
    static mut SPAWNED: Option<TaskId> = None;
    let guard = SpawnOnDrop {
        storage,
        id: &raw mut ID,
        spawned: &raw mut SPAWNED,
    };
    let dropping = storage
        .insert_task(async move {
            let _guard = guard;
        })
        .ok()
        .unwrap();
    // SAFETY: only this test's thread uses `ID`, and no reference to it exists.
    unsafe { ID = Some(dropping) };
    storage.remove_task(dropping).unwrap();
    // SAFETY: only this test's thread uses `SPAWNED`, and no reference to it exists.
    let spawned = unsafe { SPAWNED }.unwrap();
    assert_ne!(spawned.slot(), dropping.slot());
    assert!(storage.task(spawned).is_some());
}

#[test]
fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 2, 1, 1);
    let storage = backing.storage();
    let ids = [1u8, 2].map(|byte| {
        let data = [byte; TEST_CAPACITIES.max_task_size - 16];
        storage
            .insert_task(async move {
                assert!(data.iter().all(|b| *b == byte));
            })
            .ok()
            .unwrap()
    });
    let mut cx = core::task::Context::from_waker(Waker::noop());
    for id in ids {
        // SAFETY: the task is alive and not polled re-entrantly.
        assert!(unsafe { storage.task(id).unwrap().poll(&mut cx) }.is_ready());
    }
}

#[test]
#[should_panic(expected = "a task future of 8193 bytes (align 1) doesn't fit max_task_size")]
fn a_too_big_future_panics() {
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let data = [0u8; 8192];
    let _ = backing.storage().insert_task(async move {
        let _data = data;
    });
}

struct FixedFuture<T>(T);

impl<T> Future for FixedFuture<T> {
    type Output = ();

    fn poll(
        self: core::pin::Pin<&mut Self>,
        _: &mut core::task::Context<'_>,
    ) -> core::task::Poll<()> {
        core::task::Poll::Ready(())
    }
}

#[repr(align(16))]
struct Aligned16;

fn small_tasks(memory: &mut [MaybeUninit<u8>], max_tasks: usize) -> TestStorage<'_> {
    TestStorage::with_capacities(
        memory,
        Capacities {
            max_tasks,
            max_task_size: 100,
            max_timers: 1,
            max_io: 1,
        },
    )
}

#[test]
fn max_task_size_is_rounded_up_to_whole_16_byte_blocks() {
    let mut memory = stack_memory();
    let backing = small_tasks(&mut memory, 2);
    let storage = backing.storage();
    let ids = [
        storage.insert_task(FixedFuture([1u8; 112])).ok().unwrap(),
        storage
            .insert_task(FixedFuture((Aligned16, [2u8; 96])))
            .ok()
            .unwrap(),
    ];
    let mut cx = core::task::Context::from_waker(Waker::noop());
    for id in ids {
        // SAFETY: the task is alive and not polled re-entrantly.
        assert!(unsafe { storage.task(id).unwrap().poll(&mut cx) }.is_ready());
    }
}

#[test]
#[should_panic(
    expected = "a task future of 113 bytes (align 1) doesn't fit max_task_size (112 bytes"
)]
fn a_future_bigger_than_the_rounded_up_block_panics() {
    let mut memory = stack_memory();
    let backing = small_tasks(&mut memory, 1);
    let _ = backing.storage().insert_task(FixedFuture([0u8; 113]));
}

#[test]
#[should_panic(expected = "(align 32) doesn't fit max_task_size")]
fn an_overaligned_future_panics() {
    #[repr(align(32))]
    struct Aligned;
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    let aligned = Aligned;
    let _ = backing.storage().insert_task(async move {
        let _aligned = aligned;
    });
}

struct BigRoot {
    data: [u8; 9000],
    dropped: *mut bool,
}

impl Future for BigRoot {
    type Output = ();

    fn poll(
        self: core::pin::Pin<&mut Self>,
        _: &mut core::task::Context<'_>,
    ) -> core::task::Poll<()> {
        assert!(self.data.iter().all(|byte| *byte == 3));
        core::task::Poll::Pending
    }
}

impl Drop for BigRoot {
    fn drop(&mut self) {
        // SAFETY: points to a `static mut` of the running test, which only that test's thread
        // uses, and no reference to it exists.
        unsafe { *self.dropped = true };
    }
}

#[test]
fn root_region_holds_a_root_bigger_than_a_slab_block() {
    static mut DROPPED: bool = false;
    let capacities = Capacities {
        max_tasks: 2,
        max_timers: 1,
        max_io: 1,
        ..TEST_CAPACITIES
    };
    let root_layout = Layout::new::<BigRoot>();
    let mut memory = stack_memory();
    // SAFETY: `memory` is only used by this storage, and outlives it.
    let storage =
        unsafe { Storage::new(NonNull::from(&mut memory), (), (), capacities, root_layout) };
    let root = BigRoot {
        data: [3; 9000],
        dropped: &raw mut DROPPED,
    };
    // SAFETY: the only root of this storage.
    let id = unsafe { storage.spawn_root(root) };
    let sibling = storage.insert_task(async {}).ok().unwrap();
    assert_ne!(sibling.slot(), id.slot());
    let mut cx = core::task::Context::from_waker(Waker::noop());
    // SAFETY: the task is alive and not polled re-entrantly.
    assert!(unsafe { storage.task(id).unwrap().poll(&mut cx) }.is_pending());
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(!unsafe { DROPPED });
    // SAFETY: destroyed once, no task is being polled.
    unsafe { storage.destroy() };
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(unsafe { DROPPED });
}

const SMALL: Capacities = Capacities {
    max_tasks: 1,
    max_task_size: 16,
    max_timers: 1,
    max_io: 1,
};

// `len` bytes of `buffer` starting at the address that wastes the most bytes on alignment.
fn worst_aligned(buffer: &mut [MaybeUninit<u8>], len: usize) -> NonNull<[MaybeUninit<u8>]> {
    let align = StorageLayout::new::<(), ()>(SMALL, Layout::new::<()>())
        .layout
        .align();
    let start = (0..align)
        .find(|start| buffer[*start..].as_ptr().addr() % align == 1)
        .unwrap();
    NonNull::from(&mut buffer[start..start + len])
}

#[test]
fn required_size_is_enough_at_the_worst_aligned_start() {
    let root_layout = Layout::new::<()>();
    let size = Storage::<(), ()>::required_size(SMALL, root_layout);
    let mut buffer = stack_memory();
    let memory = worst_aligned(&mut buffer, size);
    // SAFETY: `memory` is only used by this storage, and outlives it.
    let storage = unsafe { Storage::new(memory, (), (), SMALL, root_layout) };
    let id = storage.insert_task(async {}).ok().unwrap();
    assert!(storage.remove_task(id).is_some());
    // SAFETY: destroyed once, no task is being polled.
    unsafe { storage.destroy() };
}

#[test]
#[should_panic(expected = "the EventLoop memory is too small")]
fn one_byte_less_than_required_panics() {
    let root_layout = Layout::new::<()>();
    let size = Storage::<(), ()>::required_size(SMALL, root_layout);
    let mut buffer = stack_memory();
    let memory = worst_aligned(&mut buffer, size - 1);
    // SAFETY: `memory` is only used by this storage (which panics before using it).
    let _ = unsafe { Storage::new(memory, (), (), SMALL, root_layout) };
}

#[test]
fn state_region_is_aligned_usable_and_dropped() {
    #[repr(align(64))]
    struct AlignedState {
        value: u32,
        _flag: DropFlag,
    }
    static mut DROPPED: bool = false;
    let capacities = Capacities {
        max_tasks: 1,
        max_timers: 1,
        max_io: 1,
        ..TEST_CAPACITIES
    };
    let root_layout = Layout::new::<()>();
    let mut memory = stack_memory();
    let state = AlignedState {
        value: 7,
        _flag: DropFlag(&raw mut DROPPED),
    };
    // SAFETY: `memory` is only used by this storage, and outlives it.
    let storage = unsafe {
        Storage::new(
            NonNull::from(&mut memory),
            (),
            state,
            capacities,
            root_layout,
        )
    };
    // SAFETY: nothing else references the state.
    let state = unsafe { &mut *storage.state().get() };
    assert_eq!((state as *const AlignedState).addr() % 64, 0);
    assert_eq!(state.value, 7);
    state.value = 8;
    // SAFETY: as above.
    assert_eq!(unsafe { &*storage.state().get() }.value, 8);
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(!unsafe { DROPPED });
    // SAFETY: destroyed once, no task is being polled.
    unsafe { storage.destroy() };
    // SAFETY: as above.
    assert!(unsafe { DROPPED });
}
