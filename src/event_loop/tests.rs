use core::{
    cell::Cell,
    future::{pending, poll_fn},
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    task::Poll,
};

use rustix::{fd::AsRawFd, io::Errno};

use crate::{
    Error, EventLoop, LoopStorage, Selector,
    testing::{
        DispatchOnDrop, EventLoopTestingExt, FdTestingExt, FlagOnDrop, TEST_EVENTS, TEST_FDS,
        TEST_TASK_SIZE, TEST_TASKS, TEST_TIMERS, TestContext, TestLoop, TestStorage, assert_yields,
        socketpair, wait_readable,
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
        async |ctx| ctx.dispatch(1).unwrap(),
    );
    assert_eq!(result.err(), Some(Error::RootFinished));
}

#[test]
fn events_dispatched_during_new_are_queued() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            ctx.dispatch(1).unwrap();
            ctx.dispatch(2).unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_yields!(event_loop.events(), [1, 2]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn idle_loop_is_not_readable_and_drains_to_nothing() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |_| pending().await,
    )
    .unwrap();
    assert!(!event_loop.is_readable_now());
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), []);
}

#[test]
#[cfg_attr(miri, ignore)]
fn root_finishing_inside_a_drain() {
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            ctx.dispatch(1).unwrap();
            wait_readable(&ctx, &rx).await;
            ctx.dispatch(2).unwrap();
            ctx.dispatch(3).unwrap();
        },
    )
    .unwrap();
    assert_yields!(event_loop.events(), [1]);

    event_loop.wake_by_writing(&tx);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_yields!(event_loop.events(), [2, 3]);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert!(!event_loop.is_readable_now());
}

#[test]
#[cfg_attr(miri, ignore)]
fn root_progresses_across_drains() {
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let mut i = 0;
            loop {
                wait_readable(&ctx, &rx).await;
                let mut buf = [0; 16];
                rustix::io::read(&rx, &mut buf).unwrap();
                ctx.dispatch(i).unwrap();
                i += 1;
            }
        },
    )
    .unwrap();
    for i in 0..3 {
        event_loop.wake_by_writing(&tx);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_yields!(event_loop.events(), [i]);
        assert!(!event_loop.is_readable_now());
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn two_loops_on_one_thread_are_independent() {
    let (rx1, tx1) = socketpair();
    let (rx2, _tx2) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut first = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            wait_readable(&ctx, &rx1).await;
            ctx.dispatch(1).unwrap();
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
        async move |ctx| {
            wait_readable(&ctx, &rx2).await;
            ctx.dispatch(2).unwrap();
        },
    )
    .unwrap();

    first.wake_by_writing(&tx1);
    assert!(!second.is_readable_now());
    assert_eq!(second.drain_events_without_blocking(), Ok(()));
    assert_eq!(
        first.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_yields!(first.events(), [1]);
    assert_yields!(second.events(), []);
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
        async |_| panic!("boom in new"),
    );
}

#[test]
#[cfg_attr(miri, ignore)]
#[should_panic(expected = "boom in drain")]
fn panic_propagates_out_of_drain() {
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            wait_readable(&ctx, &rx).await;
            panic!("boom in drain");
        },
    )
    .unwrap();
    event_loop.wake_by_writing(&tx);
    let _ = event_loop.drain_events_without_blocking();
}

#[test]
fn dispatching_into_a_full_queue_returns_the_event() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            for event in (0..).take(TEST_EVENTS) {
                ctx.dispatch(event).unwrap();
            }
            assert_eq!(ctx.dispatch(99), Err(99));
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_yields!(event_loop.events(), (0..).take(TEST_EVENTS));
}

#[test]
fn root_future_bigger_than_task_size_is_fine() {
    let data = [7u8; 3 * TEST_TASK_SIZE];
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(
        {
            static STORAGE: LoopStorage<
                u32,
                TEST_EVENTS,
                TEST_TASKS,
                TEST_TASK_SIZE,
                { 8 * TEST_TASK_SIZE },
                TEST_TIMERS,
                TEST_FDS,
            > = LoopStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            ctx.dispatch(u32::from(data.iter().all(|byte| *byte == 7)))
                .unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_yields!(event_loop.events(), [1]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn tasks_are_dropped_when_the_root_finishes() {
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let _ = ctx.spawn(async |ctx| {
                let _guard = DispatchOnDrop(&ctx, 10);
                pending::<()>().await;
            });
            let _ = ctx.spawn(async |ctx| {
                let _guard = DispatchOnDrop(&ctx, 20);
                pending::<()>().await;
            });
            wait_readable(&ctx, &rx).await;
            ctx.dispatch(1).unwrap();
        },
    )
    .unwrap();
    event_loop.wake_by_writing(&tx);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    let [first, mut rest @ ..] = event_loop.run_until_events::<3>();
    assert_eq!(first, 1);
    rest.sort_unstable();
    assert_eq!(rest, [10, 20]);
    assert_yields!(event_loop.events(), []);
}

#[test]
#[cfg_attr(miri, ignore)]
fn spawning_and_dispatching_from_drop_during_teardown() {
    struct SpawnOnDrop<'c, 'rx>(&'c TestContext<'rx>);

    impl Drop for SpawnOnDrop<'_, '_> {
        fn drop(&mut self) {
            let _ = self.0.spawn(async |ctx| ctx.dispatch(100).unwrap());
            self.0.dispatch(3).unwrap();
        }
    }

    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let _ = ctx.spawn(async |ctx| {
                let _guard = SpawnOnDrop(&ctx);
                pending::<()>().await;
            });
            wait_readable(&ctx, &rx).await;
            ctx.dispatch(1).unwrap();
        },
    )
    .unwrap();
    event_loop.wake_by_writing(&tx);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_yields!(event_loop.events(), [1, 3]);
}

#[test]
fn undrained_events_are_dropped_with_the_loop() {
    struct DispatchFlagOnDrop<'c, 'a>(
        &'c TestContext<'a, FlagOnDrop<'static>>,
        &'static AtomicBool,
    );

    impl Drop for DispatchFlagOnDrop<'_, '_> {
        fn drop(&mut self) {
            self.0.dispatch(FlagOnDrop::new(self.1)).unwrap();
        }
    }

    static QUEUED: AtomicBool = AtomicBool::new(false);
    static DISPATCHED_WHILE_DROPPING: AtomicBool = AtomicBool::new(false);
    let mut selector = Selector::new().unwrap();
    let event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage<FlagOnDrop<'static>> = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            ctx.dispatch(FlagOnDrop::new(&QUEUED)).unwrap();
            let _ = ctx.spawn(async |ctx| {
                let _guard = DispatchFlagOnDrop(&ctx, &DISPATCHED_WHILE_DROPPING);
                pending::<()>().await;
            });
            pending::<()>().await;
        },
    )
    .unwrap();
    assert!(!QUEUED.load(Relaxed));
    drop(event_loop);
    assert!(QUEUED.load(Relaxed));
    assert!(DISPATCHED_WHILE_DROPPING.load(Relaxed));
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

#[test]
#[cfg_attr(miri, ignore)]
fn loop_exposes_the_selector_fd() {
    let mut selector = Selector::new().unwrap();
    let fd = selector.as_fd().as_raw_fd();
    let event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |_| pending().await,
    )
    .unwrap();
    assert_eq!(event_loop.as_raw_fd(), fd);
}

#[test]
fn errors_display_their_cause() {
    struct StartsWith<'a>(&'a str);

    impl core::fmt::Write for StartsWith<'_> {
        fn write_str(&mut self, s: &str) -> core::fmt::Result {
            match self.0.strip_prefix(s) {
                Some(rest) => self.0 = rest,
                None if self.0.is_empty() => {}
                None => return Err(core::fmt::Error),
            }
            Ok(())
        }
    }

    for (error, expected) in [
        (Error::RootFinished, "the root future has finished"),
        (Error::Io(Errno::BADF), "event loop IO error: "),
    ] {
        let mut prefix = StartsWith(expected);
        core::fmt::write(&mut prefix, format_args!("{error}")).unwrap();
        assert_eq!(prefix.0, "", "{error:?}");
    }
}
