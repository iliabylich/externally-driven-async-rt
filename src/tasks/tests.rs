use core::{
    cell::Cell,
    future::{Future, poll_fn},
    pin::pin,
    sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed},
    task::{Context, Poll, Waker},
};

use crate::{
    tasks::{SlotWaiterTakenError, TaskId, Tasks},
    testing::{FixedFuture, FlagOnDrop, claimed_forever, counting_waker},
};

const TASK_SIZE: usize = 1024;

type TestTasks<const TASKS: usize> = Tasks<TASKS, TASK_SIZE, 0>;

fn contains<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>(
    tasks: &Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    id: TaskId<TASK_SIZE>,
) -> bool {
    let Some(entry) = tasks.pool.get_versioned(id) else {
        return false;
    };
    entry.has_task()
}

fn waker<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>(
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    id: TaskId<TASK_SIZE>,
) -> Waker {
    tasks.queue.waker(id.idx)
}

#[test]
fn dropping_a_task_drops_its_future_and_finishes_it() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let id = TASKS
        .spawn_task(claimed_forever(), FlagOnDrop::new(&DROPPED))
        .unwrap();
    assert!(contains(&TASKS, id));
    assert_eq!(TASKS.set_join_waiter(id, Waker::noop()), Poll::Pending);
    TASKS.drop_all();
    assert!(DROPPED.load(Relaxed));
    assert!(!contains(&TASKS, id));
    assert_eq!(TASKS.pool.iter().count(), 0);
    assert_eq!(TASKS.set_join_waiter(id, Waker::noop()), Poll::Ready(()));
    assert_eq!(TASKS.run_ready(), Poll::Pending);
    assert!(TASKS.queue.is_empty());
}

#[test]
fn finished_task_id_never_matches_a_reused_slot() {
    static TASKS: TestTasks<1> = TestTasks::new();
    let first = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    TASKS.drop_all();
    let second = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    assert_ne!(first, second);
    assert!(!contains(&TASKS, first));
    assert_eq!(TASKS.set_join_waiter(first, Waker::noop()), Poll::Ready(()));
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
                Poll::Ready(())
            );
            let spawned = self.tasks.spawn_task(claimed_forever(), async {}).unwrap();
            assert_ne!(spawned.idx, self.id.idx);
        }
    }

    static TASKS: TestTasks<2> = TestTasks::new();

    let (id, entry) = TASKS.pool.alloc().unwrap();
    let spawn_on_drop = SpawnOnDrop { tasks: &TASKS, id };
    TASKS.spawn_reserved(claimed_forever(), id, entry, async move {
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
        TASKS
            .spawn_task(claimed_forever(), async move {
                assert!(data.iter().all(|b| *b == byte));
            })
            .unwrap()
    });
    assert_eq!(TASKS.run_ready(), Poll::Pending);
    assert!(ids.iter().all(|id| !contains(&TASKS, *id)));
}

#[test]
fn futures_up_to_task_size_fit() {
    #[repr(align(16))]
    struct Align16;

    static TASKS: Tasks<2, 100, 0> = Tasks::new();

    let ids = [
        TASKS
            .spawn_task(claimed_forever(), FixedFuture::new([1u8; 100]))
            .unwrap(),
        TASKS
            .spawn_task(claimed_forever(), FixedFuture::new((Align16, [2u8; 80])))
            .unwrap(),
    ];
    assert_eq!(TASKS.run_ready(), Poll::Pending);
    assert!(ids.iter().all(|id| !contains(&TASKS, *id)));
}

#[test]
fn root_region_holds_a_root_bigger_than_a_slab_block() {
    struct BigRoot<'a> {
        data: [u8; 9000],
        _flag: FlagOnDrop<'a>,
    }

    impl Future for BigRoot<'_> {
        type Output = ();

        fn poll(self: core::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
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
    TASKS.spawn_root(TASKS.queue.claim().unwrap(), claimed_forever(), root);
    let _ = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    assert_eq!(TASKS.run_ready(), Poll::Pending);
    assert!(TASKS.root.has_task());
    assert!(!DROPPED.load(Relaxed));
    TASKS.drop_all();
    assert!(DROPPED.load(Relaxed));
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
    let id = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    assert_eq!(TASKS.run_ready(), Poll::Pending);
    assert!(!contains(&TASKS, id));
}

#[test]
fn a_stale_wake_of_a_freed_slot_polls_nothing() {
    static TASKS: TestTasks<1> = TestTasks::new();
    assert!(TASKS.queue.claim().is_some());
    let id = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    let waker = waker(&TASKS, id);
    let _ = TASKS.run_ready();
    waker.wake();
    assert_eq!(TASKS.run_ready(), Poll::Pending);
    assert!(TASKS.queue.is_empty());
}

#[test]
fn a_stale_wake_of_a_reserved_slot_is_skipped() {
    static TASKS: TestTasks<1> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    assert!(TASKS.queue.claim().is_some());
    let id = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    let waker = waker(&TASKS, id);
    let _ = TASKS.run_ready();
    let (id, entry) = TASKS.pool.alloc().unwrap();
    waker.wake();
    assert_eq!(TASKS.run_ready(), Poll::Pending);
    TASKS.spawn_reserved(claimed_forever(), id, entry, counting(&POLLS));
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 1);
}

#[test]
fn a_stale_wake_of_a_reused_slot_polls_its_new_task() {
    static TASKS: TestTasks<1> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    assert!(TASKS.queue.claim().is_some());
    let id = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    let waker = waker(&TASKS, id);
    let _ = TASKS.run_ready();
    let _ = TASKS
        .spawn_task(claimed_forever(), counting(&POLLS))
        .unwrap();
    let _ = TASKS.run_ready();
    waker.wake();
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    static TASKS: TestTasks<1> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    assert!(TASKS.queue.claim().is_some());
    let _ = TASKS
        .spawn_task(claimed_forever(), {
            poll_fn(|cx| {
                POLLS.fetch_add(1, Relaxed);
                if POLLS.load(Relaxed) < 3 {
                    cx.waker().wake_by_ref();
                }
                Poll::<()>::Pending
            })
        })
        .unwrap();
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let child = counting(&POLLS);
    let _ = TASKS
        .spawn_task(claimed_forever(), async move {
            let _ = TASKS.spawn_task(claimed_forever(), child).unwrap();
        })
        .unwrap();
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    assert!(TASKS.queue.claim().is_some());
    let waiter = TASKS
        .spawn_task(claimed_forever(), counting(&POLLS))
        .unwrap();
    let task = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    let _ = TASKS.set_join_waiter(task, &waker(&TASKS, waiter));
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    static TASKS: Tasks<2, TASK_SIZE, 64> = Tasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    TASKS.spawn_root(TASKS.queue.claim().unwrap(), claimed_forever(), async {});
    let _ = TASKS
        .spawn_task(claimed_forever(), counting(&POLLS))
        .unwrap();
    assert_eq!(TASKS.run_ready(), Poll::Ready(()));
    assert_eq!(POLLS.load(Relaxed), 0);
    assert!(!TASKS.root.has_task());
}

#[test]
fn releasing_a_slot_wakes_the_slot_waiter() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    assert!(TASKS.queue.claim().is_some());
    let waiter = TASKS
        .spawn_task(claimed_forever(), counting(&POLLS))
        .unwrap();
    let _ = TASKS.run_ready();
    TASKS.slot_waiter.store(&waker(&TASKS, waiter));
    let (id, _) = TASKS.pool.alloc().unwrap();
    TASKS.release(id);
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
    assert!(TASKS.slot_waiter.is_set());
}

#[test]
fn finishing_a_task_wakes_the_slot_waiter() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static POLLS: AtomicU32 = AtomicU32::new(0);
    assert!(TASKS.queue.claim().is_some());
    let waiter = TASKS
        .spawn_task(claimed_forever(), counting(&POLLS))
        .unwrap();
    let _ = TASKS.run_ready();
    TASKS.slot_waiter.store(&waker(&TASKS, waiter));
    let _ = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    let _ = TASKS.run_ready();
    assert_eq!(POLLS.load(Relaxed), 2);
}

#[test]
fn join_is_pending_until_the_task_finishes() {
    static TASKS: TestTasks<2> = TestTasks::new();
    let mut cx = Context::from_waker(Waker::noop());
    let mut join = pin!(TASKS.join(TASKS.spawn_task(claimed_forever(), async {}).unwrap()));
    assert_eq!(join.as_mut().poll(&mut cx), Poll::Pending);
    let _ = TASKS.run_ready();
    assert_eq!(join.as_mut().poll(&mut cx), Poll::Ready(()));
}

#[test]
fn joining_a_finished_task_resolves_immediately() {
    static TASKS: TestTasks<2> = TestTasks::new();
    let mut cx = Context::from_waker(Waker::noop());
    let id = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    let _ = TASKS.run_ready();
    assert_eq!(pin!(TASKS.join(id)).poll(&mut cx), Poll::Ready(()));
}

#[test]
fn dropping_a_polled_join_drops_its_join_waiter() {
    static TASKS: TestTasks<2> = TestTasks::new();
    let drops = Cell::new(0);
    {
        let waker = counting_waker(&drops);
        let mut cx = Context::from_waker(&waker);
        let mut join = pin!(TASKS.join(TASKS.spawn_task(claimed_forever(), async {}).unwrap()));
        assert_eq!(join.as_mut().poll(&mut cx), Poll::Pending);
    }
    assert_eq!(drops.get(), 2);
}

#[test]
fn dropping_a_stale_join_keeps_the_new_tasks_join_waiter() {
    static TASKS: TestTasks<2> = TestTasks::new();
    let drops = Cell::new(0);
    let mut cx = Context::from_waker(Waker::noop());
    let stale = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    {
        let mut join = pin!(TASKS.join(stale));
        assert_eq!(join.as_mut().poll(&mut cx), Poll::Pending);
        let _ = TASKS.run_ready();
        let id = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
        assert_eq!(id.idx, stale.idx);
        assert_eq!(
            TASKS.set_join_waiter(id, &counting_waker(&drops)),
            Poll::Pending
        );
        assert_eq!(drops.get(), 1);
    }
    assert_eq!(drops.get(), 1);
}

#[test]
fn dropping_a_join_detaches_the_task() {
    static TASKS: TestTasks<2> = TestTasks::new();
    static RAN: AtomicBool = AtomicBool::new(false);
    let mut cx = Context::from_waker(Waker::noop());
    let id = TASKS
        .spawn_task(claimed_forever(), async { RAN.store(true, Relaxed) })
        .unwrap();
    assert_eq!(pin!(TASKS.join(id)).poll(&mut cx), Poll::Pending);
    let _ = TASKS.run_ready();
    assert!(RAN.load(Relaxed));
}

#[test]
fn a_task_can_spawn_and_join_a_task() {
    static TASKS: TestTasks<3> = TestTasks::new();
    static STEP: AtomicU32 = AtomicU32::new(0);
    assert!(TASKS.claim().is_some());
    let _ = TASKS
        .spawn_task(claimed_forever(), async {
            TASKS
                .spawn(claimed_forever(), async {
                    TASKS
                        .spawn(claimed_forever(), async {
                            assert_eq!(STEP.fetch_add(1, Relaxed), 0);
                        })
                        .unwrap()
                        .await;
                    assert_eq!(STEP.fetch_add(1, Relaxed), 1);
                })
                .unwrap()
                .await;
            assert_eq!(STEP.fetch_add(1, Relaxed), 2);
        })
        .unwrap();
    assert_eq!(TASKS.run_ready(), Poll::Pending);
    assert_eq!(STEP.load(Relaxed), 3);
}

#[test]
fn reserve_waits_until_a_task_finishes() {
    static TASKS: TestTasks<1> = TestTasks::new();
    let mut cx = Context::from_waker(Waker::noop());
    let _ = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    let mut reserve = pin!(TASKS.reserve(claimed_forever()));
    assert!(reserve.as_mut().poll(&mut cx).is_pending());
    assert!(TASKS.slot_waiter.is_set());
    let _ = TASKS.run_ready();
    assert!(reserve.as_mut().poll(&mut cx).is_ready());
    assert!(!TASKS.slot_waiter.is_set());
}

#[test]
fn a_second_reserve_gets_an_error() {
    static TASKS: TestTasks<1> = TestTasks::new();
    let mut cx = Context::from_waker(Waker::noop());
    let _ = TASKS.spawn_task(claimed_forever(), async {}).unwrap();
    let mut first = pin!(TASKS.reserve(claimed_forever()));
    let mut second = pin!(TASKS.reserve(claimed_forever()));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(matches!(
        second.as_mut().poll(&mut cx),
        Poll::Ready(Err(SlotWaiterTakenError))
    ));
    assert!(TASKS.slot_waiter.is_set());
    let _ = TASKS.run_ready();
    assert!(first.as_mut().poll(&mut cx).is_ready());
}
