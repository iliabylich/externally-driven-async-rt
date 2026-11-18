use core::{
    future::poll_fn,
    sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed},
    task::{Poll, Waker},
};

use crate::{
    tasks::{JoinStatus, Root, RootStatus, TaskEntry, TaskId, Tasks, TasksFullError},
    testing::{
        FixedFuture, FlagOnDrop, TasksStorage, spawn_reserved_static, spawn_root_static,
        spawn_static,
    },
};

const TASK_SIZE: usize = 1024;

type Storage<const TASKS: usize> = TasksStorage<TASKS, TASK_SIZE, 0>;

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
    static STORAGE: Storage<2> = Storage::new();
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let tasks = &STORAGE.tasks;
    let id = spawn_static(tasks, FlagOnDrop::new(&DROPPED)).unwrap();
    assert!(contains(tasks, id));
    assert_eq!(
        tasks.set_join_waiter(id, Waker::noop()),
        JoinStatus::Running
    );
    tasks.drop_all();
    assert!(DROPPED.load(Relaxed));
    assert!(!contains(tasks, id));
    assert_eq!(tasks.pool.iter().count(), 0);
    assert_eq!(
        tasks.set_join_waiter(id, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
fn finished_task_id_never_matches_a_reused_slot() {
    static STORAGE: Storage<1> = Storage::new();
    let tasks = &STORAGE.tasks;
    let first = spawn_static(tasks, async {}).unwrap();
    tasks.drop_all();
    let second = spawn_static(tasks, async {}).unwrap();
    assert_ne!(first, second);
    assert!(!contains(tasks, first));
    assert_eq!(
        tasks.set_join_waiter(first, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
fn full_task_pool_rejects_a_task() {
    static STORAGE: Storage<1> = Storage::new();
    let tasks = &STORAGE.tasks;
    let _ = spawn_static(tasks, async {}).unwrap();
    assert_eq!(spawn_static(tasks, async {}), Err(TasksFullError));
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
            let spawned = spawn_static(self.tasks, async {}).unwrap();
            assert_ne!(spawned.slot(), self.id.slot());
        }
    }

    static STORAGE: Storage<2> = Storage::new();

    let tasks = &STORAGE.tasks;
    let (id, entry) = tasks.reserve().unwrap();
    let spawn_on_drop = SpawnOnDrop { tasks, id };
    spawn_reserved_static(tasks, id, entry, async move {
        let _spawn_on_drop = spawn_on_drop;
    });
    tasks.drop_all();
    assert_eq!(tasks.pool.iter().count(), 0);
}

#[test]
fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
    static STORAGE: Storage<2> = Storage::new();
    let tasks = &STORAGE.tasks;
    let ids = [1u8, 2].map(|byte| {
        let data = [byte; TASK_SIZE - 16];
        spawn_static(tasks, async move {
            assert!(data.iter().all(|b| *b == byte));
        })
        .unwrap()
    });
    assert_eq!(tasks.run_ready(), RootStatus::Running);
    assert!(ids.iter().all(|id| !contains(tasks, *id)));
}

#[test]
fn futures_up_to_task_size_fit() {
    #[repr(align(16))]
    struct Align16;

    static STORAGE: TasksStorage<2, 100, 0> = TasksStorage::new();

    let tasks = &STORAGE.tasks;
    let ids = [
        spawn_static(tasks, FixedFuture::new([1u8; 100])).unwrap(),
        spawn_static(tasks, FixedFuture::new((Align16, [2u8; 80]))).unwrap(),
    ];
    assert_eq!(tasks.run_ready(), RootStatus::Running);
    assert!(ids.iter().all(|id| !contains(tasks, *id)));
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

    static STORAGE: TasksStorage<2, TASK_SIZE, { size_of::<BigRoot<'static>>() }> =
        TasksStorage::new();

    static DROPPED: AtomicBool = AtomicBool::new(false);
    let tasks = &STORAGE.tasks;
    let root = BigRoot {
        data: [3; 9000],
        _flag: FlagOnDrop::new(&DROPPED),
    };
    spawn_root_static(tasks, root);
    let Root::Running(id) = tasks.root() else {
        panic!("spawning the root makes it running");
    };
    let sibling = spawn_static(tasks, async {}).unwrap();
    assert_ne!(sibling.slot(), id.slot());
    assert_eq!(tasks.run_ready(), RootStatus::Running);
    assert!(contains(tasks, id));
    assert!(!DROPPED.load(Relaxed));
    tasks.drop_all();
    assert!(DROPPED.load(Relaxed));
}

#[test]
#[should_panic(expected = "the root is already spawned")]
fn spawning_a_second_root_panics() {
    static STORAGE: Storage<2> = Storage::new();
    let tasks = &STORAGE.tasks;
    spawn_root_static(tasks, core::future::pending::<()>());
    spawn_root_static(tasks, core::future::pending::<()>());
}

fn counting(polls: &AtomicU32) -> impl Future<Output = ()> {
    poll_fn(move |_| {
        polls.fetch_add(1, Relaxed);
        Poll::<()>::Pending
    })
}

#[test]
fn finished_task_is_removed() {
    static STORAGE: Storage<1> = Storage::new();
    let tasks = &STORAGE.tasks;
    let id = spawn_static(tasks, async {}).unwrap();
    assert_eq!(tasks.run_ready(), RootStatus::Running);
    assert!(!contains(tasks, id));
}

#[test]
fn a_stale_wake_of_a_freed_slot_polls_nothing() {
    static STORAGE: Storage<1> = Storage::new();
    let tasks = &STORAGE.tasks;
    assert!(tasks.queue.claim());
    let id = spawn_static(tasks, async {}).unwrap();
    let waker = waker(tasks, id);
    let _ = tasks.run_ready();
    waker.wake();
    assert_eq!(tasks.run_ready(), RootStatus::Running);
    assert!(!tasks.has_ready());
}

#[test]
fn a_stale_wake_of_a_reserved_slot_is_skipped() {
    static STORAGE: Storage<1> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    assert!(tasks.queue.claim());
    let id = spawn_static(tasks, async {}).unwrap();
    let waker = waker(tasks, id);
    let _ = tasks.run_ready();
    let (id, entry) = tasks.reserve().unwrap();
    waker.wake();
    assert_eq!(tasks.run_ready(), RootStatus::Running);
    spawn_reserved_static(tasks, id, entry, counting(&POLLS));
    let _ = tasks.run_ready();
    assert_eq!(POLLS.load(Relaxed), 1);
}

#[test]
fn a_stale_wake_of_a_reused_slot_polls_its_new_task() {
    static STORAGE: Storage<1> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    assert!(tasks.queue.claim());
    let id = spawn_static(tasks, async {}).unwrap();
    let waker = waker(tasks, id);
    let _ = tasks.run_ready();
    let _ = spawn_static(tasks, counting(&POLLS)).unwrap();
    let _ = tasks.run_ready();
    waker.wake();
    let _ = tasks.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}

#[test]
fn a_task_dropped_while_queued_is_skipped() {
    static STORAGE: Storage<1> = Storage::new();
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let tasks = &STORAGE.tasks;
    let _ = spawn_static(tasks, FlagOnDrop::new(&DROPPED)).unwrap();
    tasks.drop_all();
    assert!(DROPPED.load(Relaxed));
    assert_eq!(tasks.run_ready(), RootStatus::Running);
    assert!(!tasks.has_ready());
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    static STORAGE: Storage<1> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    assert!(tasks.queue.claim());
    let _ = spawn_static(
        tasks,
        poll_fn(|cx| {
            POLLS.fetch_add(1, Relaxed);
            if POLLS.load(Relaxed) < 3 {
                cx.waker().wake_by_ref();
            }
            Poll::<()>::Pending
        }),
    )
    .unwrap();
    let _ = tasks.run_ready();
    assert_eq!(POLLS.load(Relaxed), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    static STORAGE: Storage<2> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    let child = counting(&POLLS);
    let _ = spawn_static(tasks, async move {
        let _ = spawn_static(tasks, child).unwrap();
    })
    .unwrap();
    let _ = tasks.run_ready();
    assert_eq!(POLLS.load(Relaxed), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    static STORAGE: Storage<2> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    assert!(tasks.queue.claim());
    let waiter = spawn_static(tasks, counting(&POLLS)).unwrap();
    let task = spawn_static(tasks, async {}).unwrap();
    let _ = tasks.set_join_waiter(task, &waker(tasks, waiter));
    let _ = tasks.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    static STORAGE: Storage<2> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    let root = spawn_static(tasks, async {}).unwrap();
    tasks.root.set(Root::Running(root));
    let _ = spawn_static(tasks, counting(&POLLS)).unwrap();
    assert_eq!(tasks.run_ready(), RootStatus::Finished);
    assert_eq!(POLLS.load(Relaxed), 0);
}

#[test]
fn spawning_into_a_reserved_slot_queues_the_task() {
    static STORAGE: Storage<1> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    let (id, entry) = tasks.reserve().unwrap();
    assert!(!contains(tasks, id));
    assert!(tasks.reserve().is_none());
    spawn_reserved_static(tasks, id, entry, counting(&POLLS));
    let _ = tasks.run_ready();
    assert_eq!(POLLS.load(Relaxed), 1);
    assert!(contains(tasks, id));
}

#[test]
fn releasing_a_slot_wakes_the_slot_waiter() {
    static STORAGE: Storage<2> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    assert!(tasks.queue.claim());
    let waiter = spawn_static(tasks, counting(&POLLS)).unwrap();
    let _ = tasks.run_ready();
    tasks.set_slot_waiter(&waker(tasks, waiter));
    let (id, _) = tasks.reserve().unwrap();
    tasks.release(id);
    let _ = tasks.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
    assert!(tasks.has_slot_waiter());
}

#[test]
fn finishing_a_task_wakes_the_slot_waiter() {
    static STORAGE: Storage<2> = Storage::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let tasks = &STORAGE.tasks;
    assert!(tasks.queue.claim());
    let waiter = spawn_static(tasks, counting(&POLLS)).unwrap();
    let _ = tasks.run_ready();
    tasks.set_slot_waiter(&waker(tasks, waiter));
    let _ = spawn_static(tasks, async {}).unwrap();
    let _ = tasks.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}
