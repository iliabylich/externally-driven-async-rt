use core::{
    cell::Cell,
    future::{Future, poll_fn},
    pin::pin,
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    task::{Context, Poll, Waker},
};

use crate::{
    claimed::Claimed,
    tasks::{SlotWaiterTakenError, TaskId, Tasks},
    testing::{FixedFuture, FlagOnDrop, claimed_forever, counting_waker},
};

macro_rules! tasks {
    ($tasks:tt, $task_size:tt, $root_task_size:tt) => {{
        static TASKS: Tasks<$tasks, $task_size, $root_task_size> = Tasks::new();
        let root = TASKS.queue.claim().unwrap();
        let scope = Scope::new(&TASKS);
        let claimed = scope.claimed();
        (&TASKS, root, claimed, scope)
    }};
}

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

struct Scope<'sel, const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> {
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    claimed: Claimed<'sel>,
}

impl<'sel, const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
    Scope<'sel, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    fn new(tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>) -> Self {
        Self {
            tasks,
            // SAFETY: `drop` drops every task of `tasks`, and the `Drop` impl makes the borrow
            // checker require `'sel` to outlive the scope, so every task spawned with this
            // `Claimed` is dropped before `'sel` ends. Tests never leak a scope.
            claimed: unsafe { Claimed::new() },
        }
    }

    const fn claimed(&self) -> Claimed<'sel> {
        self.claimed
    }
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> Drop
    for Scope<'_, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    fn drop(&mut self) {
        self.tasks.drop_all();
    }
}

#[test]
fn dropping_a_task_drops_its_future_and_finishes_it() {
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let id = tasks
        .spawn_task(claimed, FlagOnDrop::new(&DROPPED))
        .unwrap();
    assert!(contains(tasks, id));
    assert_eq!(tasks.set_join_waiter(id, Waker::noop()), Poll::Pending);
    tasks.drop_all();
    assert!(DROPPED.load(Relaxed));
    assert!(!contains(tasks, id));
    assert_eq!(tasks.pool.iter().count(), 0);
    assert_eq!(tasks.set_join_waiter(id, Waker::noop()), Poll::Ready(()));
    assert_eq!(tasks.run_ready(), Poll::Pending);
    assert!(tasks.queue.is_empty());
}

#[test]
fn finished_task_id_never_matches_a_reused_slot() {
    let (tasks, _root, claimed, _scope) = tasks!(1, 1024, 0);
    let first = tasks.spawn_task(claimed, async {}).unwrap();
    tasks.drop_all();
    let second = tasks.spawn_task(claimed, async {}).unwrap();
    assert_ne!(first, second);
    assert!(!contains(tasks, first));
    assert_eq!(tasks.set_join_waiter(first, Waker::noop()), Poll::Ready(()));
}

#[test]
fn a_task_spawned_while_one_is_dropped_gets_another_slot() {
    struct SpawnOnDrop {
        tasks: &'static Tasks<2, 1024, 0>,
        id: TaskId<1024>,
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

    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);

    let (id, entry) = tasks.pool.alloc().unwrap();
    let spawn_on_drop = SpawnOnDrop { tasks, id };
    tasks.spawn_reserved(claimed, id, entry, async move {
        let _spawn_on_drop = spawn_on_drop;
    });
    tasks.drop_all();
    assert_eq!(tasks.pool.iter().count(), 0);
}

#[test]
fn full_size_futures_in_neighbouring_slots_do_not_overlap() {
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let ids = [1u8, 2].map(|byte| {
        let data = [byte; 1024 - 16];
        tasks
            .spawn_task(claimed, async move {
                assert!(data.iter().all(|b| *b == byte));
            })
            .unwrap()
    });
    assert_eq!(tasks.run_ready(), Poll::Pending);
    assert!(ids.iter().all(|id| !contains(tasks, *id)));
}

#[test]
fn futures_up_to_task_size_fit() {
    #[repr(align(16))]
    struct Align16;

    let (tasks, _root, claimed, _scope) = tasks!(2, 100, 0);

    let ids = [
        tasks
            .spawn_task(claimed, FixedFuture::new([1u8; 100]))
            .unwrap(),
        tasks
            .spawn_task(claimed, FixedFuture::new((Align16, [2u8; 80])))
            .unwrap(),
    ];
    assert_eq!(tasks.run_ready(), Poll::Pending);
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

        fn poll(self: core::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            assert!(self.data.iter().all(|byte| *byte == 3));
            Poll::Pending
        }
    }

    static DROPPED: AtomicBool = AtomicBool::new(false);
    let (tasks, root, claimed, _scope) = tasks!(2, 1024, { size_of::<BigRoot<'static>>() });

    let big_root = BigRoot {
        data: [3; 9000],
        _flag: FlagOnDrop::new(&DROPPED),
    };
    tasks.spawn_root(root, claimed, big_root);
    let _ = tasks.spawn_task(claimed, async {}).unwrap();
    assert_eq!(tasks.run_ready(), Poll::Pending);
    assert!(tasks.root.has_task());
    assert!(!DROPPED.load(Relaxed));
    tasks.drop_all();
    assert!(DROPPED.load(Relaxed));
}

fn counting(polls: &Cell<u32>) -> impl Future<Output = ()> {
    poll_fn(move |_| {
        polls.set(polls.get() + 1);
        Poll::<()>::Pending
    })
}

#[test]
fn a_stale_wake_of_a_freed_slot_polls_nothing() {
    let (tasks, _root, claimed, _scope) = tasks!(1, 1024, 0);
    let id = tasks.spawn_task(claimed, async {}).unwrap();
    let waker = waker(tasks, id);
    let _ = tasks.run_ready();
    waker.wake();
    assert_eq!(tasks.run_ready(), Poll::Pending);
    assert!(tasks.queue.is_empty());
}

#[test]
fn a_stale_wake_of_a_reserved_slot_is_skipped() {
    let polls = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(1, 1024, 0);
    let id = tasks.spawn_task(claimed, async {}).unwrap();
    let waker = waker(tasks, id);
    let _ = tasks.run_ready();
    let (id, entry) = tasks.pool.alloc().unwrap();
    waker.wake();
    assert_eq!(tasks.run_ready(), Poll::Pending);
    tasks.spawn_reserved(claimed, id, entry, counting(&polls));
    let _ = tasks.run_ready();
    assert_eq!(polls.get(), 1);
}

#[test]
fn a_stale_wake_of_a_reused_slot_polls_its_new_task() {
    let polls = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(1, 1024, 0);
    let id = tasks.spawn_task(claimed, async {}).unwrap();
    let waker = waker(tasks, id);
    let _ = tasks.run_ready();
    let _ = tasks.spawn_task(claimed, counting(&polls)).unwrap();
    let _ = tasks.run_ready();
    waker.wake();
    let _ = tasks.run_ready();
    assert_eq!(polls.get(), 2);
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    let polls = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(1, 1024, 0);
    let _ = tasks
        .spawn_task(claimed, {
            poll_fn(|cx| {
                polls.set(polls.get() + 1);
                if polls.get() < 3 {
                    cx.waker().wake_by_ref();
                }
                Poll::<()>::Pending
            })
        })
        .unwrap();
    let _ = tasks.run_ready();
    assert_eq!(polls.get(), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    let polls = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let child = counting(&polls);
    let _ = tasks
        .spawn_task(claimed, async move {
            let _ = tasks.spawn_task(claimed, child).unwrap();
        })
        .unwrap();
    let _ = tasks.run_ready();
    assert_eq!(polls.get(), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    let polls = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let waiter = tasks.spawn_task(claimed, counting(&polls)).unwrap();
    let task = tasks.spawn_task(claimed, async {}).unwrap();
    let _ = tasks.set_join_waiter(task, &waker(tasks, waiter));
    let _ = tasks.run_ready();
    assert_eq!(polls.get(), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    let polls = Cell::new(0);
    let (tasks, root, claimed, _scope) = tasks!(2, 1024, 64);
    tasks.spawn_root(root, claimed, async {});
    let _ = tasks.spawn_task(claimed, counting(&polls)).unwrap();
    assert_eq!(tasks.run_ready(), Poll::Ready(()));
    assert_eq!(polls.get(), 0);
    assert!(!tasks.root.has_task());
}

#[test]
fn releasing_a_slot_wakes_the_slot_waiter() {
    let polls = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let waiter = tasks.spawn_task(claimed, counting(&polls)).unwrap();
    let _ = tasks.run_ready();
    tasks.slot_waiter.store(&waker(tasks, waiter));
    let (id, _) = tasks.pool.alloc().unwrap();
    tasks.release(id);
    let _ = tasks.run_ready();
    assert_eq!(polls.get(), 2);
    assert!(tasks.slot_waiter.is_set());
}

#[test]
fn finishing_a_task_wakes_the_slot_waiter() {
    let polls = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let waiter = tasks.spawn_task(claimed, counting(&polls)).unwrap();
    let _ = tasks.run_ready();
    tasks.slot_waiter.store(&waker(tasks, waiter));
    let _ = tasks.spawn_task(claimed, async {}).unwrap();
    let _ = tasks.run_ready();
    assert_eq!(polls.get(), 2);
}

#[test]
fn join_is_pending_until_the_task_finishes() {
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let mut cx = Context::from_waker(Waker::noop());
    let mut join = pin!(tasks.join(tasks.spawn_task(claimed, async {}).unwrap()));
    assert_eq!(join.as_mut().poll(&mut cx), Poll::Pending);
    let _ = tasks.run_ready();
    assert_eq!(join.as_mut().poll(&mut cx), Poll::Ready(()));
}

#[test]
fn joining_a_finished_task_resolves_immediately() {
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let mut cx = Context::from_waker(Waker::noop());
    let id = tasks.spawn_task(claimed, async {}).unwrap();
    let _ = tasks.run_ready();
    assert_eq!(pin!(tasks.join(id)).poll(&mut cx), Poll::Ready(()));
}

#[test]
fn dropping_a_polled_join_drops_its_join_waiter() {
    let drops = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    {
        let waker = counting_waker(&drops);
        let mut cx = Context::from_waker(&waker);
        let mut join = pin!(tasks.join(tasks.spawn_task(claimed, async {}).unwrap()));
        assert_eq!(join.as_mut().poll(&mut cx), Poll::Pending);
    }
    assert_eq!(drops.get(), 2);
}

#[test]
fn dropping_a_stale_join_keeps_the_new_tasks_join_waiter() {
    let drops = Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let mut cx = Context::from_waker(Waker::noop());
    let stale = tasks.spawn_task(claimed, async {}).unwrap();
    {
        let mut join = pin!(tasks.join(stale));
        assert_eq!(join.as_mut().poll(&mut cx), Poll::Pending);
        let _ = tasks.run_ready();
        let id = tasks.spawn_task(claimed, async {}).unwrap();
        assert_eq!(id.idx, stale.idx);
        assert_eq!(
            tasks.set_join_waiter(id, &counting_waker(&drops)),
            Poll::Pending
        );
        assert_eq!(drops.get(), 1);
    }
    assert_eq!(drops.get(), 1);
}

#[test]
fn dropping_a_join_detaches_the_task() {
    let ran = Cell::new(false);
    let (tasks, _root, claimed, _scope) = tasks!(2, 1024, 0);
    let mut cx = Context::from_waker(Waker::noop());
    let id = tasks.spawn_task(claimed, async { ran.set(true) }).unwrap();
    assert_eq!(pin!(tasks.join(id)).poll(&mut cx), Poll::Pending);
    let _ = tasks.run_ready();
    assert!(ran.get());
}

#[test]
fn a_task_can_spawn_and_join_a_task() {
    let step = &Cell::new(0);
    let (tasks, _root, claimed, _scope) = tasks!(3, 1024, 0);
    let _ = tasks
        .spawn_task(claimed, async move {
            tasks
                .spawn(claimed, async move {
                    tasks
                        .spawn(claimed, async {
                            assert_eq!(step.replace(step.get() + 1), 0);
                        })
                        .unwrap()
                        .await;
                    assert_eq!(step.replace(step.get() + 1), 1);
                })
                .unwrap()
                .await;
            assert_eq!(step.replace(step.get() + 1), 2);
        })
        .unwrap();
    assert_eq!(tasks.run_ready(), Poll::Pending);
    assert_eq!(step.get(), 3);
}

#[test]
fn reserve_waits_until_a_task_finishes() {
    let (tasks, _root, claimed, _scope) = tasks!(1, 1024, 0);
    let mut cx = Context::from_waker(Waker::noop());
    let _ = tasks.spawn_task(claimed, async {}).unwrap();
    let mut reserve = pin!(tasks.reserve(claimed));
    assert!(reserve.as_mut().poll(&mut cx).is_pending());
    assert!(tasks.slot_waiter.is_set());
    let _ = tasks.run_ready();
    assert!(reserve.as_mut().poll(&mut cx).is_ready());
    assert!(!tasks.slot_waiter.is_set());
}

#[test]
fn a_second_reserve_gets_an_error() {
    let (tasks, _root, claimed, _scope) = tasks!(1, 1024, 0);
    let mut cx = Context::from_waker(Waker::noop());
    let _ = tasks.spawn_task(claimed, async {}).unwrap();
    let mut first = pin!(tasks.reserve(claimed));
    let mut second = pin!(tasks.reserve(claimed));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(matches!(
        second.as_mut().poll(&mut cx),
        Poll::Ready(Err(SlotWaiterTakenError))
    ));
    assert!(tasks.slot_waiter.is_set());
    let _ = tasks.run_ready();
    assert!(first.as_mut().poll(&mut cx).is_ready());
}
