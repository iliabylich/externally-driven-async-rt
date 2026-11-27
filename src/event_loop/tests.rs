use core::{
    cell::Cell,
    future::{pending, poll_fn},
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    task::Poll,
};

use super::{EventLoop, EventLoopError};

use crate::{
    Context, LoopStorage, NotEnoughResources, Selector,
    testing::{DispatchOnDrop, EventLoopTestingExt, FlagOnDrop, assert_yields},
};

const EVENTS: usize = 4;
const TASKS: usize = 2;
const TASK_SIZE: usize = 64;
const ROOT_TASK_SIZE: usize = 128;

type Storage<E = u32> = LoopStorage<E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, 1, 1>;

#[test]
fn root_finishing_during_new_is_an_error() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let result = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        ctx.dispatch(1).unwrap();
    });
    assert_eq!(result.err(), Some(EventLoopError::RootFinished));
}

#[test]
fn events_dispatched_during_new_are_queued() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        ctx.dispatch(1).unwrap();
        ctx.dispatch(2).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), [1, 2]);
}

#[test]
fn root_finishing_inside_a_drain() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        ctx.dispatch(1).unwrap();
        ctx.notified().await;
        ctx.dispatch(2).unwrap();
        ctx.dispatch(3).unwrap();
    })
    .unwrap();
    assert_yields!(event_loop.events(), [1]);

    event_loop.notify().unwrap();
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(EventLoopError::RootFinished)
    );
    assert_yields!(event_loop.events(), [2, 3]);
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(EventLoopError::RootFinished)
    );
}

#[test]
fn root_progresses_across_drains() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        for i in 0..=u32::MAX {
            ctx.notified().await;
            ctx.dispatch(i).unwrap();
        }
    })
    .unwrap();
    for i in 0..3 {
        event_loop.notify().unwrap();
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
        assert_yields!(event_loop.events(), [i]);
    }
}

#[test]
fn two_loops_on_one_thread_are_independent() {
    static FIRST: Storage = Storage::new();
    static SECOND: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut first = EventLoop::new(&FIRST, &mut selector, async |ctx| {
        ctx.notified().await;
        ctx.dispatch(1).unwrap();
    })
    .unwrap();
    let mut selector = Selector::new().unwrap();
    let mut second = EventLoop::new(&SECOND, &mut selector, async |ctx| {
        ctx.notified().await;
        ctx.dispatch(2).unwrap();
    })
    .unwrap();

    first.notify().unwrap();
    assert_eq!(second.drain_events_without_blocking(), Ok(()));
    assert_eq!(
        first.drain_events_without_blocking(),
        Err(EventLoopError::RootFinished)
    );
    assert_yields!(first.events(), [1]);
    assert_yields!(second.events(), []);
}

#[test]
fn dispatching_into_a_full_queue_returns_the_event() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        for event in (0..).take(EVENTS) {
            ctx.dispatch(event).unwrap();
        }
        assert_eq!(ctx.dispatch(99), Err(99));
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), (0..).take(EVENTS));
}

#[test]
fn root_future_bigger_than_task_size_is_fine() {
    static STORAGE: LoopStorage<u32, 1, 1, TASK_SIZE, { 8 * TASK_SIZE }, 1, 1> = LoopStorage::new();
    let data = [7u8; 3 * TASK_SIZE];
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        ctx.dispatch(u32::from(data.iter().all(|byte| *byte == 7)))
            .unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), [1]);
}

#[test]
fn tasks_are_dropped_when_the_root_finishes() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let handle = ctx
            .spawn(async |ctx| {
                let _guard = DispatchOnDrop(&ctx, 10);
                pending::<()>().await;
            })
            .unwrap();
        drop(handle);
        let handle = ctx
            .spawn(async |ctx| {
                let _guard = DispatchOnDrop(&ctx, 20);
                pending::<()>().await;
            })
            .unwrap();
        drop(handle);
        ctx.notified().await;
        ctx.dispatch(1).unwrap();
    })
    .unwrap();
    event_loop.notify().unwrap();
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(EventLoopError::RootFinished)
    );
    let [first, mut rest @ ..] = event_loop.run_until_events::<3>();
    assert_eq!(first, 1);
    rest.sort_unstable();
    assert_eq!(rest, [10, 20]);
    assert_yields!(event_loop.events(), []);
}

#[test]
fn spawning_and_dispatching_from_drop_during_teardown() {
    struct SpawnOnDrop<'c, 'rx>(
        &'c Context<'rx, u32, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, 1, 1>,
    );

    impl Drop for SpawnOnDrop<'_, '_> {
        fn drop(&mut self) {
            let _ = self.0.spawn(async |ctx| ctx.dispatch(100).unwrap());
            self.0.dispatch(3).unwrap();
        }
    }

    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let handle = ctx
            .spawn(async |ctx| {
                let _guard = SpawnOnDrop(&ctx);
                pending::<()>().await;
            })
            .unwrap();
        drop(handle);
        ctx.notified().await;
        ctx.dispatch(1).unwrap();
    })
    .unwrap();
    event_loop.notify().unwrap();
    assert_eq!(
        event_loop.drain_events_without_blocking(),
        Err(EventLoopError::RootFinished)
    );
    assert_yields!(event_loop.events(), [1, 3]);
}

#[test]
fn undrained_events_are_dropped_with_the_loop() {
    struct DispatchFlagOnDrop<'c, 'sel>(
        &'c Context<'sel, FlagOnDrop<'static>, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, 1, 1>,
        &'static AtomicBool,
    );

    impl Drop for DispatchFlagOnDrop<'_, '_> {
        fn drop(&mut self) {
            self.0.dispatch(FlagOnDrop::new(self.1)).unwrap();
        }
    }

    static STORAGE: Storage<FlagOnDrop<'static>> = Storage::new();
    static QUEUED: AtomicBool = AtomicBool::new(false);
    static DISPATCHED_WHILE_DROPPING: AtomicBool = AtomicBool::new(false);
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        ctx.dispatch(FlagOnDrop::new(&QUEUED)).unwrap();
        let handle = ctx
            .spawn(async |ctx| {
                let _guard = DispatchFlagOnDrop(&ctx, &DISPATCHED_WHILE_DROPPING);
                pending::<()>().await;
            })
            .unwrap();
        drop(handle);
        pending::<()>().await;
    })
    .unwrap();
    assert!(!QUEUED.load(Relaxed));
    drop(event_loop);
    assert!(QUEUED.load(Relaxed));
    assert!(DISPATCHED_WHILE_DROPPING.load(Relaxed));
}

#[test]
fn waking_after_the_loop_is_gone_is_harmless() {
    static STORAGE: Storage = Storage::new();
    let escaped = Cell::new(None);
    {
        let mut selector = Selector::new().unwrap();
        let event_loop = EventLoop::new(&STORAGE, &mut selector, async |_| {
            poll_fn(|cx| {
                escaped.set(Some(cx.waker().clone()));
                Poll::<()>::Pending
            })
            .await;
        });
        drop(event_loop);
    }
    escaped.take().unwrap().wake();
}

#[test]
fn spawning_into_a_full_pool_drops_the_future_unpolled() {
    static STORAGE: LoopStorage<u32, 1, 1, 64, 1024, 1, 1> = LoopStorage::new();
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let polled = &Cell::new(false);
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let _running = ctx.spawn(async |_| pending().await).unwrap();
        let spawned = ctx.spawn(|_| {
            let flag = FlagOnDrop::new(&DROPPED);
            async move {
                let _flag = flag;
                polled.set(true);
            }
        });
        assert_eq!(spawned.err(), Some(NotEnoughResources::Tasks));
        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), [1]);
    assert!(DROPPED.load(Relaxed));
    assert!(!polled.get());
}

#[test]
fn a_used_storage_is_an_error_without_calling_the_closure() {
    static STORAGE: Storage = Storage::new();
    let called = Cell::new(false);
    let mut first_selector = Selector::new().unwrap();
    let _first = EventLoop::new(&STORAGE, &mut first_selector, async |_| pending().await).unwrap();
    let mut second_selector = Selector::new().unwrap();
    let second = EventLoop::new(&STORAGE, &mut second_selector, |_| {
        called.set(true);
        pending()
    });
    assert_eq!(second.err(), Some(EventLoopError::StorageAlreadyUsed));
    assert!(!called.get());
}
