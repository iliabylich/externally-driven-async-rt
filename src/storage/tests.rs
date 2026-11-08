use crate::{
    Capacities,
    instant::Instant,
    selectors::Direction,
    storage::{Storage, layout},
    test_utils::{
        DropFlag, TEST_CAPACITIES, TestStorage, counting_waker, flagged_task, stack_memory,
    },
};
use core::{alloc::Layout, cell::Cell, mem::MaybeUninit, ptr::NonNull};

#[test]
fn dropping_the_storage_drops_live_wakers() {
    let drops = Cell::new(0);
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 2, 2);
    let storage = backing.storage();
    storage.timers.alloc(Instant::now(), counting_waker(&drops));
    let io = storage.io_waiters.register(3, Direction::Read);
    let io_waker = counting_waker(&drops);
    storage.io_waiters.set_waker(io, &io_waker);
    drop(io_waker);
    assert_eq!(drops.get(), 1);
    drop(backing);
    assert_eq!(drops.get(), 3);
}

#[test]
fn dropping_the_storage_drops_live_tasks() {
    static mut DROPPED: bool = false;
    let mut memory = stack_memory();
    let backing = TestStorage::new(&mut memory, 1, 1, 1);
    backing
        .storage()
        .tasks
        .insert(flagged_task(&raw mut DROPPED));
    drop(backing);
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(unsafe { DROPPED });
}

#[test]
#[should_panic(expected = "object pool capacity must not be zero")]
fn zero_capacity_is_rejected() {
    let mut memory = stack_memory();
    TestStorage::new(&mut memory, 1, 1, 0);
}

const SMALL: Capacities = Capacities {
    max_tasks: 1,
    max_task_size: 16,
    max_timers: 1,
    max_io: 1,
};

// `len` bytes of `buffer` starting at the address that wastes the most bytes on alignment.
fn worst_aligned(buffer: &mut [MaybeUninit<u8>], len: usize) -> NonNull<[MaybeUninit<u8>]> {
    let (layout, _) = layout::plan::<(), ()>(SMALL, Layout::new::<()>());
    let align = layout.align();
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
    let id = storage.tasks.insert(async {});
    assert!(storage.tasks.remove(id).is_some());
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
    let state = unsafe { &mut *storage.state.get() };
    assert_eq!((state as *const AlignedState).addr() % 64, 0);
    assert_eq!(state.value, 7);
    state.value = 8;
    // SAFETY: as above.
    assert_eq!(unsafe { &*storage.state.get() }.value, 8);
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(!unsafe { DROPPED });
    // SAFETY: destroyed once, no task is being polled.
    unsafe { storage.destroy() };
    // SAFETY: as above.
    assert!(unsafe { DROPPED });
}
