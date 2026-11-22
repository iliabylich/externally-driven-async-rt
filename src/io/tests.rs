use core::{
    future::{Future, pending, poll_fn},
    pin::pin,
    task::{Poll, Waker},
};

use rustix::{
    fd::{AsFd, OwnedFd},
    io::Errno,
};

use crate::{
    Context, LoopStorage, Selector,
    io::IoRegistration,
    selector::Direction,
    testing::{
        EventLoopTestingExt, FdTestingExt, TestContext, TestLoop, TestStorage, assert_yields,
        socketpair,
    },
};

async fn read_some<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
>(
    ctx: &Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    fd: &OwnedFd,
) -> rustix::io::Result<usize> {
    let mut buf = [0; 64];
    let registration = IoRegistration::new(ctx, fd.as_fd(), Direction::Read).unwrap();
    loop {
        match rustix::io::read(fd, &mut buf) {
            Err(Errno::AGAIN) => registration.wait().await,
            result => return result,
        }
    }
}

async fn write_some(ctx: &TestContext<'_>, fd: &OwnedFd) -> rustix::io::Result<usize> {
    let registration = IoRegistration::new(ctx, fd.as_fd(), Direction::Write).unwrap();
    loop {
        match rustix::io::write(fd, b"x") {
            Err(Errno::AGAIN) => registration.wait().await,
            result => return result,
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn read_waits_until_the_peer_writes() {
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let n = read_some(&ctx, &rx).await.unwrap();
            ctx.dispatch(u32::try_from(n).unwrap()).unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(event_loop.registrations(), 1);
    assert!(!event_loop.is_readable_now());

    rustix::io::write(&tx, b"abc").unwrap();
    assert!(event_loop.wait_readable_blocking(1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), [3]);
    assert_eq!(event_loop.registrations(), 0);
}

#[test]
#[cfg_attr(miri, ignore)]
fn write_waits_on_a_full_buffer() {
    let (tx, rx) = socketpair();
    tx.fill_send_buffer();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let n = write_some(&ctx, &tx).await.unwrap();
            ctx.dispatch(u32::try_from(n).unwrap()).unwrap();
        },
    )
    .unwrap();
    assert_yields!(event_loop.events(), []);
    assert!(!event_loop.is_readable_now());

    rx.drain_receive_buffer();
    event_loop.run_until_root_finishes();
    assert_yields!(event_loop.events(), [1]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn read_and_write_wait_on_one_fd_at_the_same_time() {
    let (a, b) = socketpair();
    a.fill_send_buffer();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let mut read = pin!(read_some(&ctx, &a));
            let mut write = pin!(write_some(&ctx, &a));
            let (mut read_done, mut write_done) = (false, false);
            poll_fn(|cx| {
                if !read_done && let Poll::Ready(n) = read.as_mut().poll(cx) {
                    ctx.dispatch(u32::try_from(n.unwrap()).unwrap()).unwrap();
                    read_done = true;
                }
                if !write_done && let Poll::Ready(n) = write.as_mut().poll(cx) {
                    ctx.dispatch(100 + u32::try_from(n.unwrap()).unwrap())
                        .unwrap();
                    write_done = true;
                }
                if read_done && write_done {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(event_loop.registrations(), 1);
    assert!(!event_loop.is_readable_now());

    rustix::io::write(&b, b"abc").unwrap();
    assert!(event_loop.wait_readable_blocking(1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), [3]);

    b.drain_receive_buffer();
    assert!(event_loop.wait_readable_blocking(1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), [101]);
}

type IoStorage = LoopStorage<(), 1, 1, 64, 0, 1, 1>;

#[test]
#[cfg_attr(miri, ignore)]
fn dropping_a_pending_wait_deregisters() {
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector).unwrap().0
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let registration = IoRegistration::new(&ctx, rx.as_fd(), Direction::Read).unwrap();
    {
        let mut wait = pin!(registration.wait());
        assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
        ctx.flush().unwrap();
    }
    drop(registration);
    assert_eq!(ctx.storage.io_waiters.iter().count(), 0);
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(!selector.as_fd().is_readable_now());
}

#[test]
#[cfg_attr(miri, ignore)]
fn wait_survives_a_spurious_poll() {
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector).unwrap().0
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let registration = IoRegistration::new(&ctx, rx.as_fd(), Direction::Read).unwrap();
    let mut wait = pin!(registration.wait());
    assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
    assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
    ctx.flush().unwrap();
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(selector.as_fd().wait_readable_blocking(1000));
    ctx.process_selector_events().unwrap();
    assert_eq!(wait.as_mut().poll(&mut cx), Poll::Ready(()));
}

#[test]
#[cfg_attr(miri, ignore)]
fn closed_and_reused_fd_number_does_not_affect_the_new_socket() {
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector).unwrap().0
    };
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let (old_rx, _old_tx) = socketpair();
    let old = IoRegistration::new(&ctx, old_rx.as_fd(), Direction::Read).unwrap();
    {
        let mut wait = pin!(old.wait());
        assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
        ctx.flush().unwrap();
    }
    drop(old);
    drop(old_rx);

    let (rx, tx) = socketpair();
    let registration = IoRegistration::new(&ctx, rx.as_fd(), Direction::Read).unwrap();
    let mut wait = pin!(registration.wait());
    assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
    ctx.flush().unwrap();
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(selector.as_fd().wait_readable_blocking(1000));
    ctx.process_selector_events().unwrap();
    assert_eq!(wait.as_mut().poll(&mut cx), Poll::Ready(()));
}

#[test]
#[cfg_attr(miri, ignore)]
fn sequential_registrations_reuse_the_slot() {
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector).unwrap().0
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    for _ in 0..3 {
        let registration = IoRegistration::new(&ctx, rx.as_fd(), Direction::Read).unwrap();
        let mut wait = pin!(registration.wait());
        assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
        ctx.flush().unwrap();
        rustix::io::write(&tx, b"x").unwrap();
        assert!(selector.as_fd().wait_readable_blocking(1000));
        ctx.process_selector_events().unwrap();
        assert_eq!(wait.as_mut().poll(&mut cx), Poll::Ready(()));
    }
    assert_eq!(ctx.storage.io_waiters.iter().count(), 0);
}
