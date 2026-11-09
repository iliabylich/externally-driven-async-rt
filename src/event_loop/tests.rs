use core::{
    cell::Cell,
    future::{Future, pending, poll_fn},
    pin::Pin,
    task::{Poll, Waker},
    time::Duration,
};

use rustix::fd::AsRawFd;

use crate::{
    Error, EventLoop, LoopStorage, Selector, State,
    instant::Instant,
    selectors::Direction,
    storage::{LoopStorageGuard, Root, RootStatus, TaskId},
    test_utils::{
        DropFlag, Log, TEST_MAX_IO_COUNT, TEST_MAX_ROOT_TASK_SIZE, TEST_MAX_TASK_SIZE,
        TEST_MAX_TASKS_COUNT, TEST_MAX_TIMERS_COUNT, TestContext, TestLoop, TestStorage,
        assert_yields, counting_waker, is_readable, socketpair, wait_readable, wake_by_writing,
    },
};

#[test]
fn root_finishing_during_new_is_an_error() {
    let mut selector = Selector::new().unwrap();
    let result = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |ctx| ctx.dispatch(1),
    );
    assert_eq!(result.err(), Some(Error::RootFinished));
}

#[test]
fn events_dispatched_during_new_are_applied() {
    let mut selector = Selector::new().unwrap();
    let event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |ctx| {
            ctx.dispatch(1);
            ctx.dispatch(2);
            pending::<()>().await;
        },
    );
    assert_eq!(event_loop.unwrap().state_mut().events(), [1, 2]);
}

#[test]
fn idle_loop_is_not_readable_and_drains_to_nothing() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    assert!(!is_readable(&event_loop, 0));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert!(event_loop.state_mut().events().is_empty());
}

#[test]
fn root_finishing_inside_a_drain() {
    let (a, b) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            ctx.dispatch(1);
            wait_readable(ctx, &a).await;
            ctx.dispatch(2);
            ctx.dispatch(3);
        },
    )
    .unwrap();
    assert_eq!(event_loop.state_mut().events(), [1]);

    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().events(), [1, 2, 3]);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert!(!is_readable(&event_loop, 0));
}

#[test]
fn root_progresses_across_drains() {
    let (a, b) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            let mut i = 0;
            loop {
                wait_readable(ctx, &a).await;
                let mut buf = [0; 16];
                rustix::io::read(&a, &mut buf).unwrap();
                ctx.dispatch(i);
                i += 1;
            }
        },
    )
    .unwrap();
    for i in 0..3 {
        wake_by_writing(&event_loop, &b);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().events(), &[0, 1, 2][..=i as usize]);
        assert!(!is_readable(&event_loop, 0));
    }
}

#[test]
fn two_loops_on_one_thread_are_independent() {
    let (a1, b1) = socketpair();
    let (a2, _b2) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut first = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            wait_readable(ctx, &a1).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    let mut selector = Selector::new().unwrap();
    let mut second = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            wait_readable(ctx, &a2).await;
            ctx.dispatch(2);
        },
    )
    .unwrap();

    wake_by_writing(&first, &b1);
    assert!(!is_readable(&second, 0));
    assert_eq!(second.drain_events_without_blocking(), Ok(()));
    assert_eq!(
        first.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(first.state_mut().events(), [1]);
    assert!(second.state_mut().events().is_empty());
}

#[test]
#[should_panic(expected = "boom in new")]
fn panic_propagates_out_of_new() {
    let mut selector = Selector::new().unwrap();
    let _ = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| panic!("boom in new"),
    );
}

#[test]
#[should_panic(expected = "boom in drain")]
fn panic_propagates_out_of_drain() {
    let (a, b) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            wait_readable(ctx, &a).await;
            panic!("boom in drain");
        },
    )
    .unwrap();
    wake_by_writing(&event_loop, &b);
    let _ = event_loop.drain_events_without_blocking();
}

struct PanickingState;

impl State for PanickingState {
    type Event = ();

    fn apply(&mut self, (): ()) {
        panic!("boom in apply");
    }
}

#[test]
#[should_panic(expected = "boom in apply")]
fn panic_in_apply_propagates() {
    let mut selector = Selector::new().unwrap();
    let _ = TestLoop::new(
        {
            static STORAGE: TestStorage<PanickingState> = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        PanickingState,
        async |ctx| {
            ctx.dispatch(());
            pending::<()>().await;
        },
    );
}

struct DispatchOnDrop<'c, 'a>(&'c TestContext<'a>, u32);

impl Drop for DispatchOnDrop<'_, '_> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}

#[test]
fn root_future_bigger_than_task_size_is_fine() {
    let data = [7u8; 3 * TEST_MAX_TASK_SIZE];
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(
        {
            static STORAGE: LoopStorage<
                Log,
                TEST_MAX_TASKS_COUNT,
                TEST_MAX_TASK_SIZE,
                { 8 * TEST_MAX_TASK_SIZE },
                TEST_MAX_TIMERS_COUNT,
                TEST_MAX_IO_COUNT,
            > = LoopStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            ctx.dispatch(u32::from(data.iter().all(|byte| *byte == 7)));
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(event_loop.state_mut().events(), [1]);
}

#[test]
#[should_panic(expected = "more than 2 concurrent tasks")]
fn exceeding_max_tasks_count_panics() {
    let mut selector = Selector::new().unwrap();
    let _ = EventLoop::new(
        {
            static STORAGE: LoopStorage<
                Log,
                2,
                TEST_MAX_TASK_SIZE,
                TEST_MAX_ROOT_TASK_SIZE,
                TEST_MAX_TIMERS_COUNT,
                TEST_MAX_IO_COUNT,
            > = LoopStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |ctx| {
            ctx.spawn(async |_| pending().await);
            ctx.spawn(async |_| pending().await);
            pending::<()>().await;
        },
    );
}

#[test]
fn finished_tasks_free_their_slots() {
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(
        {
            static STORAGE: LoopStorage<
                Log,
                2,
                TEST_MAX_TASK_SIZE,
                TEST_MAX_ROOT_TASK_SIZE,
                TEST_MAX_TIMERS_COUNT,
                TEST_MAX_IO_COUNT,
            > = LoopStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |ctx| {
            for i in 0..5 {
                ctx.spawn(async move |ctx| ctx.dispatch(i)).await;
            }
            pending::<()>().await;
        },
    );
    assert_eq!(event_loop.unwrap().state_mut().events(), [0, 1, 2, 3, 4]);
}

#[test]
fn spawned_task_runs_in_the_same_drain() {
    let mut selector = Selector::new().unwrap();
    let event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |ctx| {
            ctx.spawn(async |ctx| ctx.dispatch(2));
            ctx.dispatch(1);
            pending::<()>().await;
        },
    );
    assert_eq!(event_loop.unwrap().state_mut().events(), [1, 2]);
}

#[test]
fn tasks_are_dropped_when_the_root_finishes() {
    let (a, b) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            ctx.spawn(async |ctx| {
                let _guard = DispatchOnDrop(ctx, 10);
                pending::<()>().await;
            });
            ctx.spawn(async |ctx| {
                let _guard = DispatchOnDrop(ctx, 20);
                pending::<()>().await;
            });
            wait_readable(ctx, &a).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    let mut state = event_loop.state_mut();
    let log = state.events_mut();
    assert_eq!(log[0], 1);
    log[1..].sort_unstable();
    assert_eq!(log[1..], [10, 20]);
}

struct SpawnOnDrop<'c, 'a>(&'c TestContext<'a>);

impl Drop for SpawnOnDrop<'_, '_> {
    fn drop(&mut self) {
        self.0.spawn(async |ctx| ctx.dispatch(100));
        self.0.dispatch(3);
    }
}

#[test]
fn spawning_and_dispatching_from_drop_during_teardown() {
    let (a, b) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            ctx.spawn(async |ctx| {
                let _guard = SpawnOnDrop(ctx);
                pending::<()>().await;
            });
            wait_readable(ctx, &a).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().events(), [1, 3]);
}

#[derive(Default)]
struct LastEvent(u32);

impl State for LastEvent {
    type Event = u32;

    fn apply(&mut self, event: u32) {
        self.0 = event;
    }
}

impl Drop for LastEvent {
    fn drop(&mut self) {
        assert_eq!(
            self.0, 2,
            "the event dispatched while dropping wasn't applied"
        );
    }
}

struct DispatchOnDropShared<'c, 'a>(&'c TestContext<'a, LastEvent>, u32);

impl Drop for DispatchOnDropShared<'_, '_> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}

#[test]
fn events_dispatched_while_dropping_the_loop_are_applied() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage<LastEvent> = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        LastEvent::default(),
        async |ctx| {
            ctx.dispatch(1);
            ctx.spawn(async |ctx| {
                let _guard = DispatchOnDropShared(ctx, 2);
                pending::<()>().await;
            });
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(event_loop.state_mut().0, 1);
    drop(event_loop);
}

#[test]
fn dropping_the_loop_drops_its_tasks() {
    let dropped = Cell::new(false);
    let flag = DropFlag(&dropped);
    let mut selector = Selector::new().unwrap();
    let event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            ctx.spawn(async move |_| {
                let _flag = flag;
                pending::<()>().await;
            });
            pending::<()>().await;
        },
    )
    .unwrap();
    assert!(!dropped.get());
    drop(event_loop);
    assert!(dropped.get());
}

#[test]
fn dropping_the_loop_drops_its_state() {
    struct Counted {
        _waker: Waker,
    }

    impl State for Counted {
        type Event = ();

        fn apply(&mut self, (): ()) {}
    }

    let drops = Cell::new(0);
    let mut selector = Selector::new().unwrap();
    let event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage<Counted> = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Counted {
            _waker: counting_waker(&drops),
        },
        async |_| {
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(drops.get(), 0);
    drop(event_loop);
    assert_eq!(drops.get(), 1);
}

#[test]
fn waking_after_the_loop_is_gone_is_harmless() {
    let escaped = Cell::new(None);
    {
        let mut selector = Selector::new().unwrap();
        let event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            Log::default(),
            async |_| {
                poll_fn(|cx| {
                    escaped.set(Some(cx.waker().clone()));
                    Poll::<()>::Pending
                })
                .await;
            },
        );
        drop(event_loop);
    }
    escaped.take().unwrap().wake();
}

fn counting(polls: &Cell<u32>) -> impl Future<Output = ()> {
    poll_fn(move |_| {
        polls.set(polls.get() + 1);
        Poll::<()>::Pending
    })
}

fn ready_tasks<'a>(event_loop: &'a TestLoop<'_, Log>) -> impl Iterator<Item = TaskId> + 'a {
    core::iter::from_fn(|| event_loop.storage.tasks.queue().pop())
}

fn parked_task(event_loop: &TestLoop<Log>) -> (TaskId, Waker) {
    let id = event_loop.storage.tasks.insert(pending());
    let waker = event_loop.storage.tasks.waker(id);
    (id, waker)
}

#[test]
fn finished_task_is_removed() {
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    let id = event_loop.storage.tasks.spawn(async {});
    assert_eq!(event_loop.storage.tasks.run_ready(), RootStatus::Running);
    assert!(!event_loop.storage.tasks.contains(id));
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    let polls = Cell::new(0);
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    event_loop.storage.tasks.spawn(poll_fn(|cx| {
        polls.set(polls.get() + 1);
        if polls.get() < 3 {
            cx.waker().wake_by_ref();
        }
        Poll::<()>::Pending
    }));
    event_loop.storage.tasks.run_ready();
    assert_eq!(polls.get(), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    let polls = Cell::new(0);
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    let storage = event_loop.storage;
    let child = counting(&polls);
    event_loop.storage.tasks.spawn(async move {
        storage.tasks.spawn(child);
    });
    event_loop.storage.tasks.run_ready();
    assert_eq!(polls.get(), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    let polls = Cell::new(0);
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    let waiter = event_loop.storage.tasks.spawn(counting(&polls));
    let task = event_loop.storage.tasks.spawn(async {});
    let waker = event_loop.storage.tasks.waker(waiter);
    event_loop.storage.tasks.set_join_waiter(task, &waker);
    event_loop.storage.tasks.run_ready();
    assert_eq!(polls.get(), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    let polls = Cell::new(0);
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    let root = event_loop.storage.tasks.spawn(async {});
    event_loop.storage.tasks.set_root(Root::Running(root));
    event_loop.storage.tasks.spawn(counting(&polls));
    assert_eq!(event_loop.storage.tasks.run_ready(), RootStatus::Finished);
    assert_eq!(polls.get(), 0);
}

struct SpawnRawOnDrop<'a> {
    storage: LoopStorageGuard<
        'a,
        Log,
        TEST_MAX_TASKS_COUNT,
        TEST_MAX_TASK_SIZE,
        TEST_MAX_ROOT_TASK_SIZE,
        TEST_MAX_TIMERS_COUNT,
        TEST_MAX_IO_COUNT,
    >,
    spawned_dropped: &'a Cell<bool>,
}

impl Future for SpawnRawOnDrop<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut core::task::Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for SpawnRawOnDrop<'_> {
    fn drop(&mut self) {
        let guard = DropFlag(self.spawned_dropped);
        self.storage.tasks.spawn(async move {
            let _guard = guard;
        });
    }
}

#[test]
fn dropping_the_loop_drops_tasks_spawned_from_drop() {
    let spawned_dropped = Cell::new(false);
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    owned.storage.tasks.spawn(SpawnRawOnDrop {
        storage: owned.storage,
        spawned_dropped: &spawned_dropped,
    });
    drop(owned);
    assert!(spawned_dropped.get());
}

#[test]
fn readable_event_wakes_the_reader_once() {
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    let (a, b) = socketpair();
    let (task, waker) = parked_task(event_loop);
    let io = event_loop
        .storage
        .io_waiters()
        .register(a.as_raw_fd(), Direction::Read);
    event_loop.storage.io_waiters().set_waker(io, &waker);
    event_loop.storage.flush().unwrap();
    rustix::io::write(&b, b"x").unwrap();
    event_loop.storage.process_selector_events().unwrap();
    assert_yields(ready_tasks(event_loop), [task]);

    rustix::io::write(&b, b"x").unwrap();
    event_loop.storage.flush().unwrap();
    assert!(!is_readable(event_loop, 0));
}

#[test]
fn removed_io_waker_arms_nothing() {
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    let (a, b) = socketpair();
    let (_, waker) = parked_task(event_loop);
    let io = event_loop
        .storage
        .io_waiters()
        .register(a.as_raw_fd(), Direction::Read);
    event_loop.storage.io_waiters().set_waker(io, &waker);
    event_loop.storage.io_waiters().remove_waker(io);
    rustix::io::write(&b, b"x").unwrap();
    event_loop.storage.flush().unwrap();
    assert!(!is_readable(event_loop, 0));
}

#[test]
fn deregistered_io_produces_no_events() {
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    let (a, b) = socketpair();
    let (_, waker) = parked_task(event_loop);
    let io = event_loop
        .storage
        .io_waiters()
        .register(a.as_raw_fd(), Direction::Read);
    event_loop.storage.io_waiters().set_waker(io, &waker);
    event_loop.storage.flush().unwrap();
    let change = event_loop.storage.io_waiters().release(io).unwrap();
    event_loop.storage.selector().apply_now(change).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(event_loop, 0));
}

#[test]
fn expired_timer_wakes_its_task() {
    let mut selector = Selector::new().unwrap();
    let owned = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async |_| pending().await,
    )
    .unwrap();
    let event_loop = &owned;
    let (task, waker) = parked_task(event_loop);
    let (_, later_waker) = parked_task(event_loop);
    let now = Instant::now();
    let storage = event_loop.storage;
    storage
        .timers()
        .alloc(now + Duration::from_millis(5), waker);
    storage
        .timers()
        .alloc(now + Duration::from_secs(60), later_waker);
    event_loop.storage.flush().unwrap();
    assert!(is_readable(event_loop, 500));
    event_loop.storage.process_selector_events().unwrap();
    assert_yields(ready_tasks(event_loop), [task]);
}
