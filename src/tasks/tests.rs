use core::{
    cell::Cell,
    cell::RefCell,
    future::poll_fn,
    task::{Poll, Waker},
};

use crate::{
    LoopStorage,
    storage::LoopStorageGuard,
    tasks::{JoinStatus, Root, RootStatus, TaskId},
    testing::{FixedFuture, FlagOnDrop},
};

const TASK_SIZE: usize = 1024;

type TasksStorage<const TASKS: usize> = LoopStorage<TASKS, TASK_SIZE, 0, 1, 1>;

#[test]
fn dropping_a_task_drops_its_future_and_finishes_it() {
    let dropped = Cell::new(false);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let tasks = storage.tasks();
    let id = storage.tasks().insert(FlagOnDrop::new(&dropped));
    assert!(tasks.contains(id));
    assert_eq!(
        tasks.set_join_waiter(id, Waker::noop()),
        JoinStatus::Running
    );
    storage.tasks().drop_all();
    assert!(dropped.get());
    assert!(!tasks.contains(id));
    assert_eq!(tasks.count(), 0);
    assert_eq!(
        tasks.set_join_waiter(id, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
fn finished_task_id_never_matches_a_reused_slot() {
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let tasks = storage.tasks();
    let first = storage.tasks().insert(async {});
    storage.tasks().drop_all();
    let second = storage.tasks().insert(async {});
    assert_ne!(first, second);
    assert!(!tasks.contains(first));
    assert_eq!(
        tasks.set_join_waiter(first, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
#[should_panic(expected = "more than 1 concurrent tasks")]
fn full_task_pool_panics() {
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    storage.tasks().insert(async {});
    storage.tasks().insert(async {});
}

#[test]
fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
    struct SpawnOnDrop<'a> {
        storage: LoopStorageGuard<'a, (), 2, TASK_SIZE, 0, 1, 1, ()>,
        id: &'a Cell<Option<TaskId>>,
        spawned: &'a Cell<Option<TaskId>>,
    }

    impl Drop for SpawnOnDrop<'_> {
        fn drop(&mut self) {
            let id = self.id.get().unwrap();
            let storage = self.storage;
            let tasks = storage.tasks();
            assert!(!tasks.contains(id));
            assert_eq!(
                tasks.set_join_waiter(id, Waker::noop()),
                JoinStatus::Finished
            );
            self.spawned.set(Some(storage.tasks().insert(async {})));
        }
    }

    let id = Cell::new(None);
    let spawned = Cell::new(None);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let tasks = storage.tasks();
    let guard = SpawnOnDrop {
        storage,
        id: &id,
        spawned: &spawned,
    };
    let dropping = storage.tasks().insert(async move {
        let _guard = guard;
    });
    id.set(Some(dropping));
    storage.tasks().drop_all();
    let spawned = spawned.get().unwrap();
    assert_ne!(spawned.slot(), dropping.slot());
    assert!(!tasks.contains(spawned));
}

#[test]
fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let tasks = storage.tasks();
    let ids = [1u8, 2].map(|byte| {
        let data = [byte; TASK_SIZE - 16];
        storage.tasks().spawn(async move {
            assert!(data.iter().all(|b| *b == byte));
        })
    });
    assert_eq!(storage.tasks().run_ready(), RootStatus::Running);
    assert!(ids.iter().all(|id| !tasks.contains(*id)));
}

#[test]
#[should_panic(expected = "a task future of 8193 bytes doesn't fit TASK_SIZE")]
fn a_too_big_future_panics() {
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let data = [0u8; 8192];
    storage.tasks().insert(async move {
        let _data = data;
    });
}

#[test]
fn futures_up_to_task_size_fit() {
    #[repr(align(16))]
    struct Align16;

    let state = RefCell::new(());
    let storage = {
        static STORAGE: LoopStorage<2, 100, 0, 1, 1> = LoopStorage::new();
        STORAGE.claim(&(), &state)
    };
    let tasks = storage.tasks();
    let ids = [
        storage.tasks().spawn(FixedFuture::new([1u8; 100])),
        storage
            .tasks()
            .spawn(FixedFuture::new((Align16, [2u8; 80]))),
    ];
    assert_eq!(storage.tasks().run_ready(), RootStatus::Running);
    assert!(ids.iter().all(|id| !tasks.contains(*id)));
}

#[test]
#[should_panic(expected = "a task future of 101 bytes doesn't fit TASK_SIZE (100 bytes)")]
fn a_future_one_byte_bigger_than_task_size_panics() {
    let state = RefCell::new(());
    let storage = {
        static STORAGE: LoopStorage<1, 100, 0, 1, 1> = LoopStorage::new();
        STORAGE.claim(&(), &state)
    };
    storage.tasks().insert(FixedFuture::new([0u8; 101]));
}

#[test]
#[should_panic(expected = "a task future aligned to 32 bytes doesn't fit (align at most 16)")]
fn an_overaligned_future_panics() {
    #[repr(align(32))]
    struct Aligned;
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let aligned = Aligned;
    storage.tasks().insert(async move {
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

    let dropped = Cell::new(false);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: LoopStorage<2, TASK_SIZE, { size_of::<BigRoot<'static>>() }, 1, 1> =
            LoopStorage::new();
        STORAGE.claim(&(), &state)
    };
    let tasks = storage.tasks();
    let root = BigRoot {
        data: [3; 9000],
        _flag: FlagOnDrop::new(&dropped),
    };
    storage.tasks().spawn_root(root);
    let Root::Running(id) = tasks.root() else {
        panic!("spawning the root makes it running");
    };
    let sibling = storage.tasks().insert(async {});
    assert_ne!(sibling.slot(), id.slot());
    assert_eq!(storage.tasks().run_ready(), RootStatus::Running);
    assert!(tasks.contains(id));
    assert!(!dropped.get());
    storage.tasks().drop_all();
    assert!(dropped.get());
}

#[test]
#[should_panic(expected = "the root future of 1 bytes doesn't fit ROOT_TASK_SIZE (0 bytes)")]
fn a_too_big_root_panics() {
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    storage.tasks().spawn_root(FixedFuture::new(0u8));
}

#[test]
#[should_panic(expected = "the root is already spawned")]
fn spawning_a_second_root_panics() {
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    storage.tasks().spawn_root(core::future::pending::<()>());
    storage.tasks().spawn_root(core::future::pending::<()>());
}

fn counting(polls: &Cell<u32>) -> impl Future<Output = ()> {
    poll_fn(move |_| {
        polls.set(polls.get() + 1);
        Poll::<()>::Pending
    })
}

#[test]
fn finished_task_is_removed() {
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let id = storage.tasks().spawn(async {});
    assert_eq!(storage.tasks().run_ready(), RootStatus::Running);
    assert!(!storage.tasks().contains(id));
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    let polls = Cell::new(0);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    storage.tasks().spawn(poll_fn(|cx| {
        polls.set(polls.get() + 1);
        if polls.get() < 3 {
            cx.waker().wake_by_ref();
        }
        Poll::<()>::Pending
    }));
    storage.tasks().run_ready();
    assert_eq!(polls.get(), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    let polls = Cell::new(0);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let tasks = storage.tasks();
    let child = counting(&polls);
    storage.tasks().spawn(async move {
        tasks.spawn(child);
    });
    storage.tasks().run_ready();
    assert_eq!(polls.get(), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    let polls = Cell::new(0);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let waiter = storage.tasks().spawn(counting(&polls));
    let task = storage.tasks().spawn(async {});
    storage
        .tasks()
        .set_join_waiter(task, &storage.tasks().waker(waiter));
    storage.tasks().run_ready();
    assert_eq!(polls.get(), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    let polls = Cell::new(0);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let root = storage.tasks().spawn(async {});
    storage.tasks().set_root(Root::Running(root));
    storage.tasks().spawn(counting(&polls));
    assert_eq!(storage.tasks().run_ready(), RootStatus::Finished);
    assert_eq!(polls.get(), 0);
}

#[test]
fn spawning_into_a_reserved_slot_queues_the_task() {
    let polls = Cell::new(0);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let (id, entry) = storage.tasks().reserve().unwrap();
    assert!(!storage.tasks().contains(id));
    assert!(storage.tasks().reserve().is_none());
    storage.tasks().spawn_reserved(id, entry, counting(&polls));
    storage.tasks().run_ready();
    assert_eq!(polls.get(), 1);
    assert!(storage.tasks().contains(id));
}

#[test]
fn releasing_a_slot_wakes_the_slot_waiter() {
    let polls = Cell::new(0);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let waiter = storage.tasks().spawn(counting(&polls));
    storage.tasks().run_ready();
    storage
        .tasks()
        .set_slot_waiter(&storage.tasks().waker(waiter));
    let (id, _) = storage.tasks().reserve().unwrap();
    storage.tasks().release(id);
    storage.tasks().run_ready();
    assert_eq!(polls.get(), 2);
    assert!(storage.tasks().has_slot_waiter());
}

#[test]
fn finishing_a_task_wakes_the_slot_waiter() {
    let polls = Cell::new(0);
    let state = RefCell::new(());
    let storage = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&(), &state)
    };
    let waiter = storage.tasks().spawn(counting(&polls));
    storage.tasks().run_ready();
    storage
        .tasks()
        .set_slot_waiter(&storage.tasks().waker(waiter));
    storage.tasks().spawn(async {});
    storage.tasks().run_ready();
    assert_eq!(polls.get(), 2);
}
