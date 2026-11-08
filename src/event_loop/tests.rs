use core::{
    future::{Future, pending, poll_fn},
    mem::MaybeUninit,
    pin::Pin,
    task::{Poll, Waker},
    time::Duration,
};

use rustix::fd::AsRawFd;

use crate::{
    Error, State,
    instant::Instant,
    selectors::Direction,
    storage::{Root, RootStatus, Storage, TaskId},
    test_utils::{
        Log, TEST_MAX_IO_COUNT, TEST_MAX_ROOT_TASK_SIZE, TEST_MAX_TASK_SIZE, TEST_MAX_TASKS_COUNT,
        TEST_MAX_TIMERS_COUNT, TEST_MEMORY, TestContext, TestLoop, assert_yields, is_readable,
        socketpair, wait_readable, test_loop, test_loop_in, test_memory_within_current_test, wake_by_writing,
    },
    waker::new_waker,
};

#[test]
fn root_finishing_during_new_is_an_error() {
    let result = test_loop!(Log::default(), async |ctx| ctx.dispatch(1),);
    assert_eq!(result.err(), Some(Error::RootFinished));
}

#[test]
fn events_dispatched_during_new_are_applied() {
    let event_loop = test_loop!(Log::default(), async |ctx| {
        ctx.dispatch(1);
        ctx.dispatch(2);
        pending::<()>().await;
    },);
    assert_eq!(event_loop.unwrap().state_mut().events(), [1, 2]);
}

#[test]
fn idle_loop_is_not_readable_and_drains_to_nothing() {
    let mut event_loop = test_loop!(Log::default(), async |_| pending().await,).unwrap();
    assert!(!is_readable(&event_loop, 0));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert!(event_loop.state_mut().events().is_empty());
}

#[test]
fn root_finishing_inside_a_drain() {
    let (a, b) = socketpair();
    let mut event_loop = test_loop!(Log::default(), async move |ctx| {
        ctx.dispatch(1);
        wait_readable(ctx, &a).await;
        ctx.dispatch(2);
        ctx.dispatch(3);
    },)
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
    let mut event_loop = test_loop!(Log::default(), async move |ctx| {
        let mut i = 0;
        loop {
            wait_readable(ctx, &a).await;
            let mut buf = [0; 16];
            rustix::io::read(&a, &mut buf).unwrap();
            ctx.dispatch(i);
            i += 1;
        }
    },)
    .unwrap();
    for i in 0..3 {
        wake_by_writing(&event_loop, &b);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().events(), &[0, 1, 2][..=i as usize]);
        assert!(!is_readable(&event_loop, 0));
    }
}

#[test]
fn handle_survives_moves_and_an_opaque_pointer_round_trip() {
    let (a, b) = socketpair();
    let event_loop = test_loop!(Log::default(), async move |ctx| {
        wait_readable(ctx, &a).await;
        ctx.dispatch(1);
    },)
    .unwrap();
    let handles = [event_loop];
    let [event_loop] = handles;
    let mut slot = MaybeUninit::new(event_loop);
    let raw: *mut TestLoop<Log> = slot.as_mut_ptr();
    // SAFETY: `raw` points to the initialized handle in `slot`, which is read out once.
    let mut event_loop = unsafe { raw.read() };
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().events(), [1]);
}

#[test]
fn two_loops_on_one_thread_are_independent() {
    let (a1, b1) = socketpair();
    let (a2, _b2) = socketpair();
    let mut first = test_loop!(Log::default(), async move |ctx| {
        wait_readable(ctx, &a1).await;
        ctx.dispatch(1);
    },)
    .unwrap();
    let mut second = test_loop!(Log::default(), async move |ctx| {
        wait_readable(ctx, &a2).await;
        ctx.dispatch(2);
    },)
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
    let _ = test_loop!(Log::default(), async |_| panic!("boom in new"),);
}

#[test]
#[should_panic(expected = "boom in drain")]
fn panic_propagates_out_of_drain() {
    let (a, b) = socketpair();
    let mut event_loop = test_loop!(Log::default(), async move |ctx| {
        wait_readable(ctx, &a).await;
        panic!("boom in drain");
    },)
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
    let _ = test_loop!(PanickingState, async |ctx| {
        ctx.dispatch(());
        pending::<()>().await;
    },);
}

struct DispatchOnDrop<'a>(&'a TestContext, u32);

impl Drop for DispatchOnDrop<'_> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}

#[test]
fn root_future_bigger_than_task_size_is_fine() {
    let data = [7u8; 3 * TEST_MAX_TASK_SIZE];
    let mut event_loop = test_loop!(
        [
            TEST_MAX_TASKS_COUNT,
            TEST_MAX_TASK_SIZE,
            8 * TEST_MAX_TASK_SIZE,
            TEST_MAX_TIMERS_COUNT,
            TEST_MAX_IO_COUNT
        ],
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
    let _ = test_loop!(
        [
            2,
            TEST_MAX_TASK_SIZE,
            TEST_MAX_ROOT_TASK_SIZE,
            TEST_MAX_TIMERS_COUNT,
            TEST_MAX_IO_COUNT
        ],
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
    let event_loop = test_loop!(
        [
            2,
            TEST_MAX_TASK_SIZE,
            TEST_MAX_ROOT_TASK_SIZE,
            TEST_MAX_TIMERS_COUNT,
            TEST_MAX_IO_COUNT
        ],
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
    let event_loop = test_loop!(Log::default(), async |ctx| {
        ctx.spawn(async |ctx| ctx.dispatch(2));
        ctx.dispatch(1);
        pending::<()>().await;
    },);
    assert_eq!(event_loop.unwrap().state_mut().events(), [1, 2]);
}

#[test]
fn tasks_are_dropped_when_the_root_finishes() {
    let (a, b) = socketpair();
    let mut event_loop = test_loop!(Log::default(), async move |ctx| {
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
    },)
    .unwrap();
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    let log = event_loop.state_mut().events_mut();
    assert_eq!(log[0], 1);
    log[1..].sort_unstable();
    assert_eq!(log[1..], [10, 20]);
}

struct SpawnOnDrop<'a>(&'a TestContext);

impl Drop for SpawnOnDrop<'_> {
    fn drop(&mut self) {
        self.0.spawn(async |ctx| ctx.dispatch(100));
        self.0.dispatch(3);
    }
}

#[test]
fn spawning_and_dispatching_from_drop_during_teardown() {
    let (a, b) = socketpair();
    let mut event_loop = test_loop!(Log::default(), async move |ctx| {
        ctx.spawn(async |ctx| {
            let _guard = SpawnOnDrop(ctx);
            pending::<()>().await;
        });
        wait_readable(ctx, &a).await;
        ctx.dispatch(1);
    },)
    .unwrap();
    wake_by_writing(&event_loop, &b);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().events(), [1, 3]);
}

struct Flag(*mut bool);

impl Drop for Flag {
    fn drop(&mut self) {
        // SAFETY: points to a `static mut` of the running test, which only that test's thread
        // uses, and no reference to it exists.
        unsafe { *self.0 = true };
    }
}

struct LastEvent(*mut u32);

impl State for LastEvent {
    type Event = u32;

    fn apply(&mut self, event: u32) {
        // SAFETY: points to a `static mut` of the running test, which only that test's thread
        // uses, and no reference to it exists.
        unsafe { *self.0 = event };
    }
}

struct DispatchOnDropShared<'a>(&'a TestContext<LastEvent>, u32);

impl Drop for DispatchOnDropShared<'_> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}

#[test]
fn events_dispatched_while_dropping_the_loop_are_applied() {
    static mut LAST: u32 = 0;
    let event_loop = test_loop!(LastEvent(&raw mut LAST), async |ctx| {
        ctx.dispatch(1);
        ctx.spawn(async |ctx| {
            let _guard = DispatchOnDropShared(ctx, 2);
            pending::<()>().await;
        });
        pending::<()>().await;
    },)
    .unwrap();
    // SAFETY: only this test's thread uses `LAST`, and no reference to it exists.
    assert_eq!(unsafe { LAST }, 1);
    drop(event_loop);
    // SAFETY: only this test's thread uses `LAST`, and no reference to it exists.
    assert_eq!(unsafe { LAST }, 2);
}

#[test]
fn dropping_the_loop_drops_its_tasks() {
    static mut DROPPED: bool = false;
    let flag = Flag(&raw mut DROPPED);
    let event_loop = test_loop!(Log::default(), async move |ctx| {
        ctx.spawn(async move |_| {
            let _flag = flag;
            pending::<()>().await;
        });
        pending::<()>().await;
    },)
    .unwrap();
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(!unsafe { DROPPED });
    drop(event_loop);
    // SAFETY: only this test's thread uses `DROPPED`, and no reference to it exists.
    assert!(unsafe { DROPPED });
}

fn parked_loop(memory: &'static mut [MaybeUninit<u8>; TEST_MEMORY]) -> TestLoop<Log> {
    test_loop_in(memory, Log::default(), async |_| pending().await).unwrap()
}

fn counting(polls: *mut u32) -> impl Future<Output = ()> {
    poll_fn(move |_| {
        // SAFETY: points to a `static mut` of the running test, which only that test's thread
        // uses, and no reference to it exists.
        unsafe { *polls += 1 };
        Poll::<()>::Pending
    })
}

fn ready_tasks(event_loop: &TestLoop<Log>) -> impl Iterator<Item = TaskId> + '_ {
    core::iter::from_fn(|| event_loop.storage.tasks.ready().pop())
}

fn parked_task(event_loop: &TestLoop<Log>) -> (TaskId, Waker) {
    let id = event_loop.storage.tasks.insert(pending());
    // SAFETY: `event_loop` outlives the waker.
    let waker = unsafe { new_waker(event_loop.storage.tasks.ready(), id) };
    (id, waker)
}

#[test]
fn finished_task_is_removed() {
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    let id = event_loop.storage.tasks.spawn(async {});
    assert_eq!(event_loop.storage.tasks.run_ready(), RootStatus::Running);
    assert!(event_loop.storage.tasks.get(id).is_none());
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    static mut POLLS: u32 = 0;
    event_loop.storage.tasks.spawn(poll_fn(move |cx| {
        // SAFETY: only this test's thread uses `POLLS`, and no reference to it exists.
        let polls = unsafe {
            POLLS += 1;
            POLLS
        };
        if polls < 3 {
            cx.waker().wake_by_ref();
        }
        Poll::<()>::Pending
    }));
    event_loop.storage.tasks.run_ready();
    // SAFETY: only this test's thread uses `POLLS`, and no reference to it exists.
    assert_eq!(unsafe { POLLS }, 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    let storage = event_loop.storage;
    static mut POLLS: u32 = 0;
    let child = counting(&raw mut POLLS);
    event_loop.storage.tasks.spawn(async move {
        storage.tasks.spawn(child);
    });
    event_loop.storage.tasks.run_ready();
    // SAFETY: only this test's thread uses `POLLS`, and no reference to it exists.
    assert_eq!(unsafe { POLLS }, 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    static mut POLLS: u32 = 0;
    let waiter = event_loop.storage.tasks.spawn(counting(&raw mut POLLS));
    let task = event_loop.storage.tasks.spawn(async {});
    // SAFETY: `event_loop` outlives the waker.
    let waker = unsafe { new_waker(event_loop.storage.tasks.ready(), waiter) };
    event_loop.storage.tasks.set_join_waiter(task, &waker);
    event_loop.storage.tasks.run_ready();
    // SAFETY: only this test's thread uses `POLLS`, and no reference to it exists.
    assert_eq!(unsafe { POLLS }, 2);
}

#[test]
fn root_finishing_stops_the_run() {
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    let root = event_loop.storage.tasks.spawn(async {});
    event_loop.storage.tasks.set_root(Root::Running(root));
    static mut POLLS: u32 = 0;
    event_loop.storage.tasks.spawn(counting(&raw mut POLLS));
    assert_eq!(event_loop.storage.tasks.run_ready(), RootStatus::Finished);
    // SAFETY: only this test's thread uses `POLLS`, and no reference to it exists.
    assert_eq!(unsafe { POLLS }, 0);
}

struct SpawnRawOnDrop {
    storage: &'static Storage<
        Log,
        TEST_MAX_TASKS_COUNT,
        TEST_MAX_TASK_SIZE,
        TEST_MAX_ROOT_TASK_SIZE,
        TEST_MAX_TIMERS_COUNT,
        TEST_MAX_IO_COUNT,
    >,
    spawned_dropped: *mut bool,
}

impl Future for SpawnRawOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut core::task::Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for SpawnRawOnDrop {
    fn drop(&mut self) {
        let guard = Flag(self.spawned_dropped);
        self.storage.tasks.spawn(async move {
            let _guard = guard;
        });
    }
}

#[test]
fn dropping_the_loop_drops_tasks_spawned_from_drop() {
    let owned = parked_loop(test_memory_within_current_test!());
    static mut SPAWNED_DROPPED: bool = false;
    owned.storage.tasks.spawn(SpawnRawOnDrop {
        storage: owned.storage,
        spawned_dropped: &raw mut SPAWNED_DROPPED,
    });
    drop(owned);
    // SAFETY: only this test's thread uses `SPAWNED_DROPPED`, and no reference to it exists.
    assert!(unsafe { SPAWNED_DROPPED });
}

#[test]
fn readable_event_wakes_the_reader_once() {
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    let (a, b) = socketpair();
    let (task, waker) = parked_task(event_loop);
    let io = event_loop
        .storage
        .io_waiters
        .register(a.as_raw_fd(), Direction::Read);
    event_loop.storage.io_waiters.set_waker(io, &waker);
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
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    let (a, b) = socketpair();
    let (_, waker) = parked_task(event_loop);
    let io = event_loop
        .storage
        .io_waiters
        .register(a.as_raw_fd(), Direction::Read);
    event_loop.storage.io_waiters.set_waker(io, &waker);
    event_loop.storage.io_waiters.remove_waker(io);
    rustix::io::write(&b, b"x").unwrap();
    event_loop.storage.flush().unwrap();
    assert!(!is_readable(event_loop, 0));
}

#[test]
fn deregistered_io_produces_no_events() {
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    let (a, b) = socketpair();
    let (_, waker) = parked_task(event_loop);
    let io = event_loop
        .storage
        .io_waiters
        .register(a.as_raw_fd(), Direction::Read);
    event_loop.storage.io_waiters.set_waker(io, &waker);
    event_loop.storage.flush().unwrap();
    let change = event_loop.storage.io_waiters.release(io).unwrap();
    event_loop.storage.selector.apply_now(change).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(event_loop, 0));
}

#[test]
fn expired_timer_wakes_its_task() {
    let owned = parked_loop(test_memory_within_current_test!());
    let event_loop = &owned;
    let (task, waker) = parked_task(event_loop);
    let (_, later_waker) = parked_task(event_loop);
    let now = Instant::now();
    let storage = event_loop.storage;
    storage.timers.alloc(now + Duration::from_millis(5), waker);
    storage
        .timers
        .alloc(now + Duration::from_secs(60), later_waker);
    event_loop.storage.flush().unwrap();
    assert!(is_readable(event_loop, 500));
    event_loop.storage.process_selector_events().unwrap();
    assert_yields(ready_tasks(event_loop), [task]);
}
