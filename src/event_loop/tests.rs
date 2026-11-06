use super::*;
use core::future::{Future, pending, poll_fn};
use core::task::Poll;
use core::time::Duration;
use std::rc::Rc;

use crate::test_utils::{
    Log, TEST_MAX_IO, TEST_MAX_TASKS, TEST_MAX_TIMERS, TestContext, is_readable, pair, readable,
    wake_by_writing,
};

#[test]
fn root_finishing_during_new_is_an_error() {
    let result = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |ctx| ctx.dispatch(1),
    );
    assert_eq!(result.err(), Some(Error::RootFinished));
}

#[test]
fn events_dispatched_during_new_are_applied() {
    let event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |ctx| {
            ctx.dispatch(1);
            ctx.dispatch(2);
            pending::<()>().await;
        },
    );
    assert_eq!(event_loop.unwrap().as_mut().state_mut().0, vec![1, 2]);
}

#[test]
fn idle_loop_is_not_readable_and_drains_to_nothing() {
    let mut event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |_| pending().await,
    )
    .unwrap();
    assert!(!is_readable(&*event_loop, 0));
    assert_eq!(event_loop.as_mut().drain_events_without_blocking(), Ok(()));
    assert!(event_loop.as_mut().state_mut().0.is_empty());
}

#[test]
fn root_finishing_inside_a_drain() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async move |ctx| {
            ctx.dispatch(1);
            readable(ctx, &a).await;
            ctx.dispatch(2);
            ctx.dispatch(3);
        },
    )
    .unwrap();
    assert_eq!(event_loop.as_mut().state_mut().0, vec![1]);

    wake_by_writing(&*event_loop, &b);
    assert_eq!(
        event_loop.as_mut().drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.as_mut().state_mut().0, vec![1, 2, 3]);
    assert_eq!(
        event_loop.as_mut().drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert!(!is_readable(&*event_loop, 0));
}

#[test]
fn root_progresses_across_drains() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async move |ctx| {
            let mut i = 0;
            loop {
                readable(ctx, &a).await;
                let mut buf = [0; 16];
                rustix::io::read(&a, &mut buf).unwrap();
                ctx.dispatch(i);
                i += 1;
            }
        },
    )
    .unwrap();
    for i in 0..3 {
        wake_by_writing(&*event_loop, &b);
        assert_eq!(event_loop.as_mut().drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.as_mut().state_mut().0.pop(), Some(i));
        assert!(!is_readable(&*event_loop, 0));
    }
}

#[test]
fn handle_survives_moves_and_an_ffi_round_trip() {
    let (a, b) = pair();
    let event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async move |ctx| {
            readable(ctx, &a).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    let handles = vec![event_loop];
    let event_loop = handles.into_iter().next().unwrap();
    // SAFETY: the loop is turned back into the same `Pin<Box>` right away and never moved.
    let raw = Box::into_raw(unsafe { Pin::into_inner_unchecked(event_loop) });
    // SAFETY: `raw` comes from `Box::into_raw` of a pinned box.
    let mut event_loop = unsafe { Pin::new_unchecked(Box::from_raw(raw)) };
    wake_by_writing(&*event_loop, &b);
    assert_eq!(
        event_loop.as_mut().drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.as_mut().state_mut().0, vec![1]);
}

#[test]
fn two_loops_on_one_thread_are_independent() {
    let (a1, b1) = pair();
    let (a2, _b2) = pair();
    let mut first = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async move |ctx| {
            readable(ctx, &a1).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    let mut second = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async move |ctx| {
            readable(ctx, &a2).await;
            ctx.dispatch(2);
        },
    )
    .unwrap();

    wake_by_writing(&*first, &b1);
    assert!(!is_readable(&*second, 0));
    assert_eq!(second.as_mut().drain_events_without_blocking(), Ok(()));
    assert_eq!(
        first.as_mut().drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(first.as_mut().state_mut().0, vec![1]);
    assert!(second.as_mut().state_mut().0.is_empty());
}

#[test]
#[should_panic(expected = "boom in new")]
fn panic_propagates_out_of_new() {
    let _ = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |_| panic!("boom in new"),
    );
}

#[test]
#[should_panic(expected = "boom in drain")]
fn panic_propagates_out_of_drain() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async move |ctx| {
            readable(ctx, &a).await;
            panic!("boom in drain");
        },
    )
    .unwrap();
    wake_by_writing(&*event_loop, &b);
    let _ = event_loop.as_mut().drain_events_without_blocking();
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
    let _ = EventLoop::new(
        PanickingState,
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |ctx| {
            ctx.dispatch(());
            pending::<()>().await;
        },
    );
}

struct DispatchOnDrop<'a>(&'a TestContext, u32);

impl Drop for DispatchOnDrop<'_> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}

#[test]
#[should_panic(expected = "more than 2 concurrent tasks")]
fn exceeding_max_tasks_count_panics() {
    let _ = EventLoop::new(
        Log::default(),
        2,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |ctx| {
            ctx.spawn(async |_| pending().await);
            ctx.spawn(async |_| pending().await);
            pending::<()>().await;
        },
    );
}

#[test]
fn finished_tasks_free_their_slots() {
    let event_loop = EventLoop::new(
        Log::default(),
        2,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |ctx| {
            for i in 0..5 {
                ctx.spawn(async move |ctx| ctx.dispatch(i)).await;
            }
            pending::<()>().await;
        },
    );
    assert_eq!(
        event_loop.unwrap().as_mut().state_mut().0,
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn spawned_task_runs_in_the_same_drain() {
    let event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |ctx| {
            ctx.spawn(async |ctx| ctx.dispatch(2));
            ctx.dispatch(1);
            pending::<()>().await;
        },
    );
    assert_eq!(event_loop.unwrap().as_mut().state_mut().0, vec![1, 2]);
}

#[test]
fn tasks_are_dropped_when_the_root_finishes() {
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async move |ctx| {
            ctx.spawn(async |ctx| {
                let _guard = DispatchOnDrop(ctx, 10);
                pending::<()>().await;
            });
            ctx.spawn(async |ctx| {
                let _guard = DispatchOnDrop(ctx, 20);
                pending::<()>().await;
            });
            readable(ctx, &a).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    wake_by_writing(&*event_loop, &b);
    assert_eq!(
        event_loop.as_mut().drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    let log = &mut event_loop.as_mut().state_mut().0;
    assert_eq!(log[0], 1);
    log[1..].sort();
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
    let (a, b) = pair();
    let mut event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async move |ctx| {
            ctx.spawn(async |ctx| {
                let _guard = SpawnOnDrop(ctx);
                pending::<()>().await;
            });
            readable(ctx, &a).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    wake_by_writing(&*event_loop, &b);
    assert_eq!(
        event_loop.as_mut().drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.as_mut().state_mut().0, vec![1, 3]);
}

struct Flag(Rc<Cell<bool>>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

struct SharedLog(Rc<RefCell<Vec<u32>>>);

impl State for SharedLog {
    type Event = u32;

    fn apply(&mut self, event: u32) {
        self.0.borrow_mut().push(event);
    }
}

struct DispatchOnDropShared<'a>(&'a Context<SharedLog>, u32);

impl Drop for DispatchOnDropShared<'_> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}

#[test]
fn events_dispatched_while_dropping_the_loop_are_applied() {
    let log = Rc::new(RefCell::new(Vec::new()));
    let event_loop = EventLoop::new(
        SharedLog(log.clone()),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
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
    assert_eq!(*log.borrow(), vec![1]);
    drop(event_loop);
    assert_eq!(*log.borrow(), vec![1, 2]);
}

#[test]
fn dropping_the_loop_drops_its_tasks() {
    let dropped = Rc::new(Cell::new(false));
    let flag = Flag(dropped.clone());
    let event_loop = EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
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

fn parked_loop() -> Pin<Box<EventLoop<Log>>> {
    EventLoop::new(
        Log::default(),
        TEST_MAX_TASKS,
        TEST_MAX_TIMERS,
        TEST_MAX_IO,
        async |_| pending().await,
    )
    .unwrap()
}

fn counting(polls: &Rc<Cell<u32>>) -> TaskFuture {
    let polls = polls.clone();
    Box::pin(poll_fn(move |_| {
        polls.set(polls.get() + 1);
        Poll::<()>::Pending
    }))
}

fn ready_tasks(event_loop: &EventLoop<Log>) -> Vec<TaskId> {
    core::iter::from_fn(|| event_loop.storage.borrow().ready().pop()).collect()
}

fn parked_task(event_loop: &EventLoop<Log>) -> (TaskId, Waker) {
    let id = event_loop
        .storage
        .borrow_mut()
        .insert_task(Box::pin(pending()))
        .ok()
        .unwrap();
    // SAFETY: `event_loop` outlives the waker.
    let waker = unsafe { new_waker(event_loop.storage.borrow().ready(), id) };
    (id, waker)
}

#[test]
fn finished_task_is_removed() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let (root, _) = parked_task(event_loop);
    let id = event_loop.spawn(Box::pin(async {}));
    assert_eq!(event_loop.run_ready_tasks(root), RootStatus::Running);
    assert!(event_loop.storage.borrow().task(id).is_none());
}

#[test]
fn task_woken_during_its_poll_is_polled_again_in_the_same_run() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let (root, _) = parked_task(event_loop);
    let polls = Rc::new(Cell::new(0));
    let counter = polls.clone();
    event_loop.spawn(Box::pin(poll_fn(move |cx| {
        counter.set(counter.get() + 1);
        if counter.get() < 3 {
            cx.waker().wake_by_ref();
        }
        Poll::<()>::Pending
    })));
    event_loop.run_ready_tasks(root);
    assert_eq!(polls.get(), 3);
}

#[test]
fn task_spawned_during_a_poll_runs_in_the_same_run() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let (root, _) = parked_task(event_loop);
    let ptr: *const EventLoop<Log> = event_loop;
    let polls = Rc::new(Cell::new(0));
    let child = counting(&polls);
    event_loop.spawn(Box::pin(async move {
        // SAFETY: tasks are dropped before the loop's fields are.
        unsafe { &*ptr }.spawn(child);
    }));
    event_loop.run_ready_tasks(root);
    assert_eq!(polls.get(), 1);
}

#[test]
fn join_waiter_is_woken_when_the_task_finishes() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let (root, _) = parked_task(event_loop);
    let polls = Rc::new(Cell::new(0));
    let waiter = event_loop.spawn(counting(&polls));
    let task = event_loop.spawn(Box::pin(async {}));
    // SAFETY: `event_loop` outlives the waker.
    let waker = unsafe { new_waker(event_loop.storage.borrow().ready(), waiter) };
    event_loop
        .storage
        .borrow_mut()
        .set_join_waiter(task, &waker);
    event_loop.run_ready_tasks(root);
    assert_eq!(polls.get(), 2);
}

#[test]
fn root_finishing_stops_the_run() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let root = event_loop.spawn(Box::pin(async {}));
    let polls = Rc::new(Cell::new(0));
    event_loop.spawn(counting(&polls));
    assert_eq!(event_loop.run_ready_tasks(root), RootStatus::Finished);
    assert_eq!(polls.get(), 0);
}

struct SpawnRawOnDrop {
    event_loop: *const EventLoop<Log>,
    spawned_dropped: Rc<Cell<bool>>,
}

impl Future for SpawnRawOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut core::task::Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for SpawnRawOnDrop {
    fn drop(&mut self) {
        let flag = self.spawned_dropped.clone();
        let guard = DropGuard(flag);
        // SAFETY: tasks are dropped before the loop's fields are.
        unsafe { &*self.event_loop }.spawn(Box::pin(async move {
            let _guard = guard;
        }));
    }
}

struct DropGuard(Rc<Cell<bool>>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn dropping_the_loop_drops_tasks_spawned_from_drop() {
    let owned = parked_loop();
    let spawned_dropped = Rc::new(Cell::new(false));
    owned.spawn(Box::pin(SpawnRawOnDrop {
        event_loop: &*owned,
        spawned_dropped: spawned_dropped.clone(),
    }));
    drop(owned);
    assert!(spawned_dropped.get());
}

#[test]
fn readable_event_wakes_the_reader_once() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let (a, b) = pair();
    let (task, waker) = parked_task(event_loop);
    let io = event_loop.register_io(a.as_fd(), Direction::Read);
    event_loop.set_io_waker(io, &waker);
    event_loop.flush().unwrap();
    rustix::io::write(&b, b"x").unwrap();
    event_loop.process_selector_events().unwrap();
    assert_eq!(ready_tasks(event_loop), vec![task]);

    rustix::io::write(&b, b"x").unwrap();
    event_loop.flush().unwrap();
    assert!(!is_readable(event_loop, 0));
}

#[test]
fn removed_io_waker_arms_nothing() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let (a, b) = pair();
    let (_, waker) = parked_task(event_loop);
    let io = event_loop.register_io(a.as_fd(), Direction::Read);
    event_loop.set_io_waker(io, &waker);
    event_loop.remove_io_waker(io);
    rustix::io::write(&b, b"x").unwrap();
    event_loop.flush().unwrap();
    assert!(!is_readable(event_loop, 0));
}

#[test]
fn deregistered_io_produces_no_events() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let (a, b) = pair();
    let (_, waker) = parked_task(event_loop);
    let io = event_loop.register_io(a.as_fd(), Direction::Read);
    event_loop.set_io_waker(io, &waker);
    event_loop.flush().unwrap();
    event_loop.deregister_io(io).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(event_loop, 0));
}

#[test]
fn expired_timer_wakes_its_task() {
    let owned = parked_loop();
    let event_loop = &*owned;
    let (task, waker) = parked_task(event_loop);
    let (_, later_waker) = parked_task(event_loop);
    let now = Instant::now();
    let mut storage = event_loop.storage.borrow_mut();
    storage.alloc_timer(now + Duration::from_millis(5), waker);
    storage.alloc_timer(now + Duration::from_secs(60), later_waker);
    drop(storage);
    event_loop.flush().unwrap();
    assert!(is_readable(event_loop, 500));
    event_loop.process_selector_events().unwrap();
    assert_eq!(ready_tasks(event_loop), vec![task]);
}
