use core::{
    future::poll_fn,
    sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed},
    task::{Poll, Waker},
};

use crate::{
    tasks::{JoinStatus, Root, RootStatus, TaskEntry, TaskId, Tasks},
    testing::{FixedFuture, FlagOnDrop, spawn_reserved_static, spawn_root_static, spawn_static},
};

const TASK_SIZE: usize = 1024;

type TestTasks<const TASKS: usize> = Tasks<TASKS, TASK_SIZE, 0>;

fn contains<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>(
    tasks: &Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    id: TaskId<TASK_SIZE>,
) -> bool {
    tasks.pool.get(id).is_some_and(TaskEntry::has_task)
}

fn waker<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>(
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    id: TaskId<TASK_SIZE>,
) -> Waker {
    tasks.queue.waker(id.slot())
}

#[test]
fn dropping_a_task_drops_its_future_and_finishes_it() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let id = spawn_static(&TASKS, FlagOnDrop::new(&DROPPED));
    assert!(contains(&TASKS, id));
    assert_eq!(
        TASKS.set_join_waiter(id, Waker::noop()),
        JoinStatus::Running
    );
    TASKS.drop_all();
    assert!(DROPPED.load(Relaxed));
    assert!(!contains(&TASKS, id));
    assert_eq!(TASKS.pool.iter().count(), 0);
    assert_eq!(
        TASKS.set_join_waiter(id, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
fn finished_task_id_never_matches_a_reused_slot() {
    static TASKS: TestTasks<1> = TestTasks::new();
    let first = spawn_static(&TASKS, async {});
    TASKS.drop_all();
    let second = spawn_static(&TASKS, async {});
    assert_ne!(first, second);
    assert!(!contains(&TASKS, first));
    assert_eq!(
        TASKS.set_join_waiter(first, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
#[should_panic(expected = "more than 1 concurrent tasks")]
fn full_task_pool_panics() {
    static TASKS: TestTasks<1> = TestTasks::new();
    let _ = spawn_static(&TASKS, async {});
    let _ = spawn_static(&TASKS, async {});
}

#[test]
fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
    struct SpawnOnDrop {
        tasks: &'static Tasks<2, TASK_SIZE, 0>,
        id: TaskId<TASK_SIZE>,
    }

    impl Drop for SpawnOnDrop {
        fn drop(&mut self) {
            assert!(!contains(self.tasks, self.id));
            assert_eq!(
                self.tasks.set_join_waiter(self.id, Waker::noop()),
                JoinStatus::Finished
            );
            let spawned = spawn_static(self.tasks, async {});
            assert_ne!(spawned.slot(), self.id.slot());
        }
    }

    static TASKS: TestTasks<2> = TestTasks::new();
    let (id, entry) = TASKS.reserve().unwrap();
    let spawn_on_drop = SpawnOnDrop { tasks: &TASKS, id };
    spawn_reserved_static(&TASKS, id, entry, async move {
        let _spawn_on_drop = spawn_on_drop;
    });
    TASKS.drop_all();
    assert_eq!(TASKS.pool.iter().count(), 0);
}

#[test]
fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
    static TASKS: TestTasks<2> = TestTasks::new();
    let ids = [1u8, 2].map(|byte| {
        let data = [byte; TASK_SIZE - 16];
        spawn_static(&TASKS, async move {
            assert!(data.iter().all(|b| *b == byte));
        })
    });
    assert_eq!(TASKS.run_ready(), RootStatus::Running);
    assert!(ids.iter().all(|id| !contains(&TASKS, *id)));
}

#[test]
#[should_panic(expected = "a task future of 8193 bytes doesn't fit TASK_SIZE")]
fn a_too_big_future_panics() {
    static TASKS: TestTasks<1> = TestTasks::new();
    let data = [0u8; 8192];
    let _ = spawn_static(&TASKS, async move {
        let _data = data;
    });
}

#[test]
fn futures_up_to_task_size_fit() {
    #[repr(align(16))]
    struct Align16;

    static TASKS: Tasks<2, 100, 0> = Tasks::new();
    let ids = [
        spawn_static(&TASKS, FixedFuture::new([1u8; 100])),
        spawn_static(&TASKS, FixedFuture::new((Align16, [2u8; 80]))),
    ];
    assert_eq!(TASKS.run_ready(), RootStatus::Running);
    assert!(ids.iter().all(|id| !contains(&TASKS, *id)));
}

#[test]
#[should_panic(expected = "a task future of 101 bytes doesn't fit TASK_SIZE (100 bytes)")]
fn a_future_one_byte_bigger_than_task_size_panics() {
    static TASKS: Tasks<1, 100, 0> = Tasks::new();
    let _ = spawn_static(&TASKS, FixedFuture::new([0u8; 101]));
}

#[test]
#[should_panic(expected = "a task future aligned to 32 bytes doesn't fit (align at most 16)")]
fn an_overaligned_future_panics() {
    #[repr(align(32))]
    struct Aligned;
    static TASKS: TestTasks<1> = TestTasks::new();
    let aligned = Aligned;
    let _ = spawn_static(&TASKS, async move {
        let _aligned = aligned;
    });
}

#[test]
fn root_region_holds_a_root_bigger_than_a_slab_block() {
    struct BigRoot<'a> {
        data: [u8; 9000],
        _flag: FlagOnDrop<'a>,
    }

    impl Future for BigRoot<'_> {
        type Output = ();

        fn poll(self: core::pin::Pin<&mut Self>, _: &mut core::task::Context<'_>) -> Poll<()> {
            assert!(self.data.iter().all(|byte| *byte == 3));
            Poll::Pending
        }
    }

    static TASKS: Tasks<2, TASK_SIZE, { size_of::<BigRoot<'static>>() }> = Tasks::new();
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let root = BigRoot {
        data: [3; 9000],
        _flag: FlagOnDrop::new(&DROPPED),
    };
    spawn_root_static(&TASKS, root);
    let Root::Running(id) = TASKS.root() else {
        panic!("spawning the root makes it running");
    };
    let sibling = spawn_static(&TASKS, async {});
    assert_ne!(sibling.slot(), id.slot());
    assert_eq!(TASKS.run_ready(), RootStatus::Running);
    assert!(contains(&TASKS, id));
    assert!(!DROPPED.load(Relaxed));
    TASKS.drop_all();
    assert!(DROPPED.load(Relaxed));
}

#[test]
#[should_panic(expected = "the root future of 1 bytes doesn't fit ROOT_TASK_SIZE (0 bytes)")]
fn a_too_big_root_panics() {
    static TASKS: TestTasks<1> = TestTasks::new();
    spawn_root_static(&TASKS, FixedFuture::new(0u8));
}

#[test]
#[should_panic(expected = "the root is already spawned")]
fn spawning_a_second_root_panics() {
    static TASKS: TestTasks<2> = TestTasks::new();
    spawn_root_static(&TASKS, core::future::pending::<()>());
    spawn_root_static(&TASKS, core::future::pending::<()>());
}

fn counting(polls: &AtomicU32) -> impl Future<Output = ()> {
    poll_fn(move |_| {
        polls.fetch_add(1, Relaxed);
        Poll::<()>::Pending
    })
}

#[test]
fn finished_task_is_removed() {
    static TASKS: TestTasks<1> = TestTasks::new();
    let id = spawn_static(&TASKS, async {});
    assert_eq!(TASKS.run_ready(), RootStatus::Running);
    assert!(!contains(&TASKS, id));
}

#[test]
fn a_stale_wake_of_a_freed_slot_polls_nothing() {
    static TASKS: TestTasks<1> = TestTasks::new();
    TASKS.queue.claim();
    let id = spawn_static(&TASKS, async {});
    let waker = waker(&TASKS, id);
    let _ = TASKS.run_ready();
    waker.wake();
    assert_eq!(TASKS.run_ready(), RootStatus::Running);
    assert!(!TASKS.has_ready());
}

#[test]
fn a_stale_wake_of_a_reserved_slot_is_skipped() {
    static TASKS: TestTasks<1> = TestTasks::new();
    TASKS.queue.claim();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let id = spawn_static(&TASKS, async {});
    let waker = waker(&TASKS, id);
    let _ = TASKS.run_ready();
    let (id, entry) = TASKS.reserve().unwrap();
    waker.wake();
    assert_eq!(TASKS.run_ready(), RootStatus::Running);
    spawn_reserved_static(&TASKS, id, entry, counting(&POLLS));
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 1);
}

#[test]
fn a_stale_wake_of_a_reused_slot_polls_its_new_task() {
    static TASKS: TestTasks<1> = TestTasks::new();
    TASKS.queue.claim();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let id = spawn_static(&TASKS, async {});
    let waker = waker(&TASKS, id);
    let _ = TASKS.run_ready();
    let _ = spawn_static(&TASKS, counting(&POLLS));
    let _ = TASKS.run_ready();
    waker.wake();
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}

#[test]
fn a_task_dropped_while_queued_is_skipped() {
    static TASKS: TestTasks<1> = TestTasks::new();
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let _ = spawn_static(&TASKS, FlagOnDrop::new(&DROPPED));
    TASKS.drop_all();
    assert!(DROPPED.load(Relaxed));
    assert_eq!(TASKS.run_ready(), RootStatus::Running);
    assert!(!TASKS.has_ready());
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    static TASKS: TestTasks<1> = TestTasks::new();
    TASKS.queue.claim();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let _ = spawn_static(
        &TASKS,
        poll_fn(|cx| {
            POLLS.fetch_add(1, Relaxed);
            if POLLS.load(Relaxed) < 3 {
                cx.waker().wake_by_ref();
            }
            Poll::<()>::Pending
        }),
    );
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let child = counting(&POLLS);
    let _ = spawn_static(&TASKS, async move {
        let _ = spawn_static(&TASKS, child);
    });
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    static TASKS: TestTasks<2> = TestTasks::new();
    TASKS.queue.claim();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let waiter = spawn_static(&TASKS, counting(&POLLS));
    let task = spawn_static(&TASKS, async {});
    let _ = TASKS.set_join_waiter(task, &waker(&TASKS, waiter));
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let root = spawn_static(&TASKS, async {});
    TASKS.root.set(Root::Running(root));
    let _ = spawn_static(&TASKS, counting(&POLLS));
    assert_eq!(TASKS.run_ready(), RootStatus::Finished);
    assert_eq!(POLLS.load(Relaxed), 0);
}

#[test]
fn spawning_into_a_reserved_slot_queues_the_task() {
    static TASKS: TestTasks<1> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let (id, entry) = TASKS.reserve().unwrap();
    assert!(!contains(&TASKS, id));
    assert!(TASKS.reserve().is_none());
    spawn_reserved_static(&TASKS, id, entry, counting(&POLLS));
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 1);
    assert!(contains(&TASKS, id));
}

#[test]
fn releasing_a_slot_wakes_the_slot_waiter() {
    static TASKS: TestTasks<2> = TestTasks::new();
    TASKS.queue.claim();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let waiter = spawn_static(&TASKS, counting(&POLLS));
    let _ = TASKS.run_ready();
    TASKS.set_slot_waiter(&waker(&TASKS, waiter));
    let (id, _) = TASKS.reserve().unwrap();
    TASKS.release(id);
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
    assert!(TASKS.has_slot_waiter());
}

#[test]
fn finishing_a_task_wakes_the_slot_waiter() {
    static TASKS: TestTasks<2> = TestTasks::new();
    TASKS.queue.claim();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let waiter = spawn_static(&TASKS, counting(&POLLS));
    let _ = TASKS.run_ready();
    TASKS.set_slot_waiter(&waker(&TASKS, waiter));
    let _ = spawn_static(&TASKS, async {});
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}
