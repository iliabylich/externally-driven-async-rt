use crate::{EventLoop, LoopStorage, Selector, testing::EventLoopTestingExt};

use super::FdExt;

type Storage = LoopStorage<u32, 1, 1, 0, 128, 1, 1>;

#[test]
fn notify_makes_the_loop_readable_and_wakes_the_waiter() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        for i in 1..=u32::MAX {
            ctx.notified().await;
            ctx.dispatch(i).unwrap();
        }
    })
    .unwrap();
    assert!(!event_loop.is_readable_now());

    event_loop.notify().unwrap();
    assert_eq!(event_loop.run_until_events(), [1]);
    assert!(!event_loop.is_readable_now());

    event_loop.notify().unwrap();
    assert_eq!(event_loop.run_until_events(), [2]);
    assert!(!event_loop.is_readable_now());
}

#[test]
fn notifications_coalesce() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        for i in 1..=u32::MAX {
            ctx.notified().await;
            ctx.dispatch(i).unwrap();
        }
    })
    .unwrap();

    event_loop.notify().unwrap();
    event_loop.notify().unwrap();
    event_loop.notify().unwrap();
    assert_eq!(event_loop.run_until_events(), [1]);
    assert!(!event_loop.is_readable_now());
}
