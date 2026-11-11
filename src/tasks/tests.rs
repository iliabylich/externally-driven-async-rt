use core::{
    cell::{Cell, RefCell},
    future::poll_fn,
    task::{Poll, Waker},
};

use crate::{
    LoopStorage, Selector,
    context::Context,
    tasks::{JoinStatus, Root, RootStatus, TaskId},
    testing::{FixedFuture, FlagOnDrop},
};

const TASK_SIZE: usize = 1024;

type TasksStorage<const TASKS: usize> = LoopStorage<TASKS, TASK_SIZE, 0, 1, 1>;

#[test]
fn dropping_a_task_drops_its_future_and_finishes_it() {
    let dropped = Cell::new(false);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let id = ctx.insert(FlagOnDrop::new(&dropped));
    assert!(ctx.contains(id));
    assert_eq!(ctx.set_join_waiter(id, Waker::noop()), JoinStatus::Running);
    ctx.drop_all();
    assert!(dropped.get());
    assert!(!ctx.contains(id));
    assert_eq!(ctx.count(), 0);
    assert_eq!(ctx.set_join_waiter(id, Waker::noop()), JoinStatus::Finished);
}

#[test]
fn finished_task_id_never_matches_a_reused_slot() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let first = ctx.insert(async {});
    ctx.drop_all();
    let second = ctx.insert(async {});
    assert_ne!(first, second);
    assert!(!ctx.contains(first));
    assert_eq!(
        ctx.set_join_waiter(first, Waker::noop()),
        JoinStatus::Finished
    );
}

#[test]
#[should_panic(expected = "more than 1 concurrent tasks")]
fn full_task_pool_panics() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    ctx.insert(async {});
    ctx.insert(async {});
}

#[test]
fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
    struct SpawnOnDrop<'a> {
        ctx: Context<'a, (), 2, TASK_SIZE, 0, 1, 1>,
        id: &'a Cell<Option<TaskId<TASK_SIZE>>>,
        spawned: &'a Cell<Option<TaskId<TASK_SIZE>>>,
    }

    impl Drop for SpawnOnDrop<'_> {
        fn drop(&mut self) {
            let id = self.id.get().unwrap();
            let ctx = self.ctx;
            assert!(!ctx.contains(id));
            assert_eq!(ctx.set_join_waiter(id, Waker::noop()), JoinStatus::Finished);
            self.spawned.set(Some(ctx.insert(async {})));
        }
    }

    let id = Cell::new(None);
    let spawned = Cell::new(None);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let spawn_on_drop = SpawnOnDrop {
        ctx,
        id: &id,
        spawned: &spawned,
    };
    let dropping = ctx.insert(async move {
        let _spawn_on_drop = spawn_on_drop;
    });
    id.set(Some(dropping));
    ctx.drop_all();
    let spawned = spawned.get().unwrap();
    assert_ne!(spawned.slot(), dropping.slot());
    assert!(!ctx.contains(spawned));
}

#[test]
fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let ids = [1u8, 2].map(|byte| {
        let data = [byte; TASK_SIZE - 16];
        ctx.spawn_future(async move {
            assert!(data.iter().all(|b| *b == byte));
        })
    });
    assert_eq!(ctx.run_ready(), RootStatus::Running);
    assert!(ids.iter().all(|id| !ctx.contains(*id)));
}

#[test]
#[should_panic(expected = "a task future of 8193 bytes doesn't fit TASK_SIZE")]
fn a_too_big_future_panics() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let data = [0u8; 8192];
    ctx.insert(async move {
        let _data = data;
    });
}

#[test]
fn futures_up_to_task_size_fit() {
    #[repr(align(16))]
    struct Align16;

    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: LoopStorage<2, 100, 0, 1, 1> = LoopStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let ids = [
        ctx.spawn_future(FixedFuture::new([1u8; 100])),
        ctx.spawn_future(FixedFuture::new((Align16, [2u8; 80]))),
    ];
    assert_eq!(ctx.run_ready(), RootStatus::Running);
    assert!(ids.iter().all(|id| !ctx.contains(*id)));
}

#[test]
#[should_panic(expected = "a task future of 101 bytes doesn't fit TASK_SIZE (100 bytes)")]
fn a_future_one_byte_bigger_than_task_size_panics() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: LoopStorage<1, 100, 0, 1, 1> = LoopStorage::new();
        STORAGE.claim(&selector, &state)
    };
    ctx.insert(FixedFuture::new([0u8; 101]));
}

#[test]
#[should_panic(expected = "a task future aligned to 32 bytes doesn't fit (align at most 16)")]
fn an_overaligned_future_panics() {
    #[repr(align(32))]
    struct Aligned;
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let aligned = Aligned;
    ctx.insert(async move {
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
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: LoopStorage<2, TASK_SIZE, { size_of::<BigRoot<'static>>() }, 1, 1> =
            LoopStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let root = BigRoot {
        data: [3; 9000],
        _flag: FlagOnDrop::new(&dropped),
    };
    ctx.spawn_root(root);
    let Root::Running(id) = ctx.root() else {
        panic!("spawning the root makes it running");
    };
    let sibling = ctx.insert(async {});
    assert_ne!(sibling.slot(), id.slot());
    assert_eq!(ctx.run_ready(), RootStatus::Running);
    assert!(ctx.contains(id));
    assert!(!dropped.get());
    ctx.drop_all();
    assert!(dropped.get());
}

#[test]
#[should_panic(expected = "the root future of 1 bytes doesn't fit ROOT_TASK_SIZE (0 bytes)")]
fn a_too_big_root_panics() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    ctx.spawn_root(FixedFuture::new(0u8));
}

#[test]
#[should_panic(expected = "the root is already spawned")]
fn spawning_a_second_root_panics() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    ctx.spawn_root(core::future::pending::<()>());
    ctx.spawn_root(core::future::pending::<()>());
}

fn counting(polls: &Cell<u32>) -> impl Future<Output = ()> {
    poll_fn(move |_| {
        polls.set(polls.get() + 1);
        Poll::<()>::Pending
    })
}

#[test]
fn finished_task_is_removed() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let id = ctx.spawn_future(async {});
    assert_eq!(ctx.run_ready(), RootStatus::Running);
    assert!(!ctx.contains(id));
}

#[test]
fn a_stale_wake_of_a_freed_slot_polls_nothing() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let id = ctx.spawn_future(async {});
    let waker = ctx.waker(id);
    ctx.run_ready();
    waker.wake();
    assert_eq!(ctx.run_ready(), RootStatus::Running);
    assert!(!ctx.has_ready());
}

#[test]
fn a_stale_wake_of_a_reserved_slot_is_skipped() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let id = ctx.spawn_future(async {});
    let waker = ctx.waker(id);
    ctx.run_ready();
    let (id, entry) = ctx.reserve().unwrap();
    waker.wake();
    assert_eq!(ctx.run_ready(), RootStatus::Running);
    ctx.spawn_reserved(id, entry, counting(&polls));
    ctx.run_ready();
    assert_eq!(polls.get(), 1);
}

#[test]
fn a_stale_wake_of_a_reused_slot_polls_its_new_task() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let id = ctx.spawn_future(async {});
    let waker = ctx.waker(id);
    ctx.run_ready();
    ctx.spawn_future(counting(&polls));
    ctx.run_ready();
    waker.wake();
    ctx.run_ready();
    assert_eq!(polls.get(), 2);
}

#[test]
fn a_task_dropped_while_queued_is_skipped() {
    let dropped = Cell::new(false);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    ctx.spawn_future(FlagOnDrop::new(&dropped));
    ctx.drop_all();
    assert!(dropped.get());
    assert_eq!(ctx.run_ready(), RootStatus::Running);
    assert!(!ctx.has_ready());
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    ctx.spawn_future(poll_fn(|cx| {
        polls.set(polls.get() + 1);
        if polls.get() < 3 {
            cx.waker().wake_by_ref();
        }
        Poll::<()>::Pending
    }));
    ctx.run_ready();
    assert_eq!(polls.get(), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let child = counting(&polls);
    ctx.spawn_future(async move {
        ctx.spawn_future(child);
    });
    ctx.run_ready();
    assert_eq!(polls.get(), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let waiter = ctx.spawn_future(counting(&polls));
    let task = ctx.spawn_future(async {});
    ctx.set_join_waiter(task, &ctx.waker(waiter));
    ctx.run_ready();
    assert_eq!(polls.get(), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let root = ctx.spawn_future(async {});
    ctx.set_root(Root::Running(root));
    ctx.spawn_future(counting(&polls));
    assert_eq!(ctx.run_ready(), RootStatus::Finished);
    assert_eq!(polls.get(), 0);
}

#[test]
fn spawning_into_a_reserved_slot_queues_the_task() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<1> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let (id, entry) = ctx.reserve().unwrap();
    assert!(!ctx.contains(id));
    assert!(ctx.reserve().is_none());
    ctx.spawn_reserved(id, entry, counting(&polls));
    ctx.run_ready();
    assert_eq!(polls.get(), 1);
    assert!(ctx.contains(id));
}

#[test]
fn releasing_a_slot_wakes_the_slot_waiter() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let waiter = ctx.spawn_future(counting(&polls));
    ctx.run_ready();
    ctx.set_slot_waiter(&ctx.waker(waiter));
    let (id, _) = ctx.reserve().unwrap();
    ctx.release(id);
    ctx.run_ready();
    assert_eq!(polls.get(), 2);
    assert!(ctx.has_slot_waiter());
}

#[test]
fn finishing_a_task_wakes_the_slot_waiter() {
    let polls = Cell::new(0);
    let selector = Selector::new().unwrap();
    let state = RefCell::new(());
    let ctx = {
        static STORAGE: TasksStorage<2> = TasksStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let waiter = ctx.spawn_future(counting(&polls));
    ctx.run_ready();
    ctx.set_slot_waiter(&ctx.waker(waiter));
    ctx.spawn_future(async {});
    ctx.run_ready();
    assert_eq!(polls.get(), 2);
}
