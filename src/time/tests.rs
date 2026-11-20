use core::{
    future::{Future, pending},
    pin::pin,
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    Selector,
    tasks::RootStatus,
    testing::{
        FlagOnDrop, TestContext, TestStorage, assert_yields, sleep_blocking, wake_expired_timers,
    },
};

#[test]
fn future_finishing_first_wins() {
    let selector = Selector::new().unwrap();
    let ctx: TestContext<'_> = {
        static STORAGE: TestStorage = TestStorage::new();
        STORAGE.claim(&selector).unwrap()
    };
    ctx.storage.tasks.spawn_root(ctx.claimed, async move {
        let result = ctx
            .timeout(Duration::from_secs(60), ctx.notified())
            .await
            .unwrap();
        ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
        pending::<()>().await;
    });
    assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
    assert!(ctx.storage.timers.next_deadline().is_some());
    ctx.storage.notification.fire();
    assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
    assert_yields!(ctx.storage.events.iter(), [1]);
    assert_eq!(ctx.storage.timers.next_deadline(), None);
    ctx.clear();
}

#[test]
fn elapsing_drops_the_future() {
    static DROPPED: AtomicBool = AtomicBool::new(false);
    let selector = Selector::new().unwrap();
    let ctx: TestContext<'_> = {
        static STORAGE: TestStorage = TestStorage::new();
        STORAGE.claim(&selector).unwrap()
    };
    ctx.storage.tasks.spawn_root(ctx.claimed, async move {
        let result = ctx
            .timeout(Duration::from_millis(1), async {
                let _flag = FlagOnDrop::new(&DROPPED);
                pending::<()>().await;
            })
            .await
            .unwrap();
        ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
        pending::<()>().await;
    });
    assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
    assert!(!DROPPED.load(Relaxed));
    sleep_blocking(Duration::from_millis(1));
    wake_expired_timers(&ctx.storage.timers);
    assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
    assert_yields!(ctx.storage.events.iter(), [0]);
    assert!(DROPPED.load(Relaxed));
    assert_eq!(ctx.storage.timers.next_deadline(), None);
    ctx.clear();
}

#[test]
fn ready_future_beats_a_zero_timeout() {
    static STORAGE: TestStorage = TestStorage::new();
    let selector = Selector::new().unwrap();
    let ctx: TestContext<'_> = STORAGE.claim(&selector).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut timeout = pin!(ctx.timeout(Duration::ZERO, async {
        let value = 1;
        let borrowed = &value;
        core::future::ready(()).await;
        *borrowed
    }));
    assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Ready(Ok(Some(1))));
    assert_eq!(ctx.storage.timers.next_deadline(), None);
}

#[test]
fn pending_pinned_future_keeps_its_timer_until_dropped() {
    static STORAGE: TestStorage = TestStorage::new();
    let selector = Selector::new().unwrap();
    let ctx: TestContext<'_> = STORAGE.claim(&selector).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    {
        let mut timeout = pin!(ctx.timeout(Duration::from_secs(60), async {
            let value = 1;
            let borrowed = &value;
            pending::<()>().await;
            *borrowed
        }));
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
        assert!(ctx.storage.timers.next_deadline().is_some());
    }
    assert_eq!(ctx.storage.timers.next_deadline(), None);
}
