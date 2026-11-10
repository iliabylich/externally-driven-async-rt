use core::{
    cell::Cell,
    future::{pending, poll_fn},
    task::{Poll, Waker},
};

use rustix::{fd::AsRawFd, io::Errno};

use crate::{
    Error, EventLoop, LoopStorage, Selector, State,
    testing::{
        DispatchOnDrop, EventLoopTestingExt, Log, TEST_FDS, TEST_TASK_SIZE, TEST_TASKS,
        TEST_TIMERS, TestContext, TestLoop, TestStorage, counting_waker, is_readable, socketpair,
        wait_readable,
    },
};

#[test]
#[cfg_attr(miri, ignore)]
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
#[cfg_attr(miri, ignore)]
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
#[cfg_attr(miri, ignore)]
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
        Log::default(),
        async move |ctx| {
            ctx.dispatch(1);
            wait_readable(&ctx, &rx).await;
            ctx.dispatch(2);
            ctx.dispatch(3);
        },
    )
    .unwrap();
    assert_eq!(event_loop.state_mut().events(), [1]);

    event_loop.wake_by_writing(&tx);
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
        Log::default(),
        async move |ctx| {
            let mut i = 0;
            loop {
                wait_readable(&ctx, &rx).await;
                let mut buf = [0; 16];
                rustix::io::read(&rx, &mut buf).unwrap();
                ctx.dispatch(i);
                i += 1;
            }
        },
    )
    .unwrap();
    for i in 0..3 {
        event_loop.wake_by_writing(&tx);
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_eq!(event_loop.state_mut().events(), &[0, 1, 2][..=i as usize]);
        assert!(!is_readable(&event_loop, 0));
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
        Log::default(),
        async move |ctx| {
            wait_readable(&ctx, &rx1).await;
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
            wait_readable(&ctx, &rx2).await;
            ctx.dispatch(2);
        },
    )
    .unwrap();

    first.wake_by_writing(&tx1);
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
#[cfg_attr(miri, ignore)]
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
        Log::default(),
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
#[cfg_attr(miri, ignore)]
#[should_panic(expected = "boom in apply")]
fn panic_in_apply_propagates() {
    struct PanickingState;

    impl State for PanickingState {
        type Event = ();

        fn apply(&mut self, (): ()) {
            panic!("boom in apply");
        }
    }

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

#[test]
#[cfg_attr(miri, ignore)]
fn root_future_bigger_than_task_size_is_fine() {
    let data = [7u8; 3 * TEST_TASK_SIZE];
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(
        {
            static STORAGE: LoopStorage<
                Log,
                TEST_TASKS,
                TEST_TASK_SIZE,
                { 8 * TEST_TASK_SIZE },
                TEST_TIMERS,
                TEST_FDS,
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
        Log::default(),
        async move |ctx| {
            ctx.spawn(async |ctx| {
                let _guard = DispatchOnDrop(&ctx, 10);
                pending::<()>().await;
            });
            ctx.spawn(async |ctx| {
                let _guard = DispatchOnDrop(&ctx, 20);
                pending::<()>().await;
            });
            wait_readable(&ctx, &rx).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    event_loop.wake_by_writing(&tx);
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

#[test]
#[cfg_attr(miri, ignore)]
fn spawning_and_dispatching_from_drop_during_teardown() {
    struct SpawnOnDrop<'c, 'rx>(&'c TestContext<'rx>);

    impl Drop for SpawnOnDrop<'_, '_> {
        fn drop(&mut self) {
            self.0.spawn(async |ctx| ctx.dispatch(100));
            self.0.dispatch(3);
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
        Log::default(),
        async move |ctx| {
            ctx.spawn(async |ctx| {
                let _guard = SpawnOnDrop(&ctx);
                pending::<()>().await;
            });
            wait_readable(&ctx, &rx).await;
            ctx.dispatch(1);
        },
    )
    .unwrap();
    event_loop.wake_by_writing(&tx);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(Error::RootFinished)
    );
    assert_eq!(event_loop.state_mut().events(), [1, 3]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn events_dispatched_while_dropping_the_loop_are_applied() {
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
                let _guard = DispatchOnDrop(&ctx, 2);
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
#[cfg_attr(miri, ignore)]
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
#[cfg_attr(miri, ignore)]
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
        Log::default(),
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
