use crate::{
    Selector,
    testing::{EventLoopTestingExt, FdTestingExt, TestLoop, TestStorage},
};

#[test]
fn notify_makes_the_loop_readable_and_wakes_the_waiter() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            for i in 1..=u32::MAX {
                ctx.notified().await;
                ctx.dispatch(i).unwrap();
            }
        },
    )
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
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            for i in 1..=u32::MAX {
                ctx.notified().await;
                ctx.dispatch(i).unwrap();
            }
        },
    )
    .unwrap();

    event_loop.notify().unwrap();
    event_loop.notify().unwrap();
    event_loop.notify().unwrap();
    assert_eq!(event_loop.run_until_events(), [1]);
    assert!(!event_loop.is_readable_now());
}
