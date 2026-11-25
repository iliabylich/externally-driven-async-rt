use core::{
    cell::Cell,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use std::panic::catch_unwind;

use externally_driven_async_rt::{EventLoop, EventLoopError, LoopStorage, Selector};

type Storage = LoopStorage<(), 1, 1, 64, 256, 1, 1>;

#[test]
fn a_panicking_drain_poisons_the_loop() {
    static STORAGE: Storage = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        ctx.notified().await;
        panic!("the root panics");
    })
    .unwrap();
    event_loop.notify().unwrap();
    let drained = catch_unwind(AssertUnwindSafe(|| {
        event_loop.drain_events_without_blocking()
    }));
    assert!(drained.is_err());
    for _ in 0..2 {
        assert_eq!(
            event_loop.drain_events_without_blocking(),
            Err(EventLoopError::Poisoned)
        );
    }
}

#[test]
fn dropping_a_poisoned_loop_drops_the_panicked_future() {
    struct FlagOnDrop<'a>(&'a Cell<bool>);

    impl Drop for FlagOnDrop<'_> {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    struct PanicsOnSecondPoll<'a> {
        waker: &'a Cell<Option<Waker>>,
        polled: bool,
        _flag: FlagOnDrop<'a>,
    }

    impl Future for PanicsOnSecondPoll<'_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let this = self.get_mut();
            assert!(!this.polled, "the root panics");
            this.polled = true;
            this.waker.set(Some(cx.waker().clone()));
            Poll::Pending
        }
    }

    static STORAGE: Storage = Storage::new();
    let dropped = Cell::new(false);
    let waker = Cell::new(None);
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, |_| PanicsOnSecondPoll {
        waker: &waker,
        polled: false,
        _flag: FlagOnDrop(&dropped),
    })
    .unwrap();
    waker.take().unwrap().wake();
    let drained = catch_unwind(AssertUnwindSafe(|| {
        event_loop.drain_events_without_blocking()
    }));
    assert!(drained.is_err());
    assert!(!dropped.get());
    drop(event_loop);
    assert!(dropped.get());
}
