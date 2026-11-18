use core::{
    cell::Cell,
    future::{Future, pending, poll_fn},
    pin::{Pin, pin},
    task::{Poll, Waker},
};

use rustix::{
    fd::{AsFd, OwnedFd},
    io::Errno,
};

use crate::{
    Context, LoopStorage, Selector,
    io::{IoIndex, IoRegistration},
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
    cached: &Cell<Option<IoIndex>>,
) -> rustix::io::Result<usize> {
    let mut buf = [0; 64];
    let mut registration = IoRegistration::new(ctx, fd.as_fd(), cached, Direction::Read);
    loop {
        match rustix::io::read(fd, &mut buf) {
            Err(Errno::AGAIN) => registration.wait().await,
            result => return result,
        }
    }
}

async fn write_some(
    ctx: &TestContext<'_>,
    fd: &OwnedFd,
    cached: &Cell<Option<IoIndex>>,
) -> rustix::io::Result<usize> {
    let mut registration = IoRegistration::new(ctx, fd.as_fd(), cached, Direction::Write);
    loop {
        match rustix::io::write(fd, b"x") {
            Err(Errno::AGAIN) => registration.wait().await,
            result => return result,
        }
    }
}

fn fill_send_buffer(fd: &OwnedFd) {
    while rustix::io::write(fd, &[0; 4096]).is_ok() {}
}

fn drain_receive_buffer(fd: &OwnedFd) {
    let mut buf = [0; 4096];
    while rustix::io::read(fd, &mut buf).is_ok() {}
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
            let n = read_some(&ctx, &rx, &Cell::new(None)).await.unwrap();
            ctx.dispatch(n as u32).unwrap();
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
    fill_send_buffer(&tx);
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let n = write_some(&ctx, &tx, &Cell::new(None)).await.unwrap();
            ctx.dispatch(n as u32).unwrap();
        },
    )
    .unwrap();
    assert_yields!(event_loop.events(), []);
    assert!(!event_loop.is_readable_now());

    drain_receive_buffer(&rx);
    event_loop.run_until_root_finishes();
    assert_yields!(event_loop.events(), [1]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn read_and_write_wait_on_one_fd_at_the_same_time() {
    let (a, b) = socketpair();
    fill_send_buffer(&a);
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let cached = Cell::new(None);
            let mut read = pin!(read_some(&ctx, &a, &cached));
            let mut write = pin!(write_some(&ctx, &a, &cached));
            let (mut read_done, mut write_done) = (false, false);
            poll_fn(|cx| {
                if !read_done && let Poll::Ready(n) = read.as_mut().poll(cx) {
                    ctx.dispatch(n.unwrap() as u32).unwrap();
                    read_done = true;
                }
                if !write_done && let Poll::Ready(n) = write.as_mut().poll(cx) {
                    ctx.dispatch(100 + n.unwrap() as u32).unwrap();
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

    drain_receive_buffer(&b);
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
        STORAGE.claim(&selector)
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let mut registration = IoRegistration::new(&ctx, rx.as_fd(), &Cell::new(None), Direction::Read);
    let mut wait = registration.wait();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    ctx.flush().unwrap();
    drop(wait);
    drop(registration);
    assert_eq!(ctx.storage.io_waiters.count(), 0);
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(!selector.as_fd().is_readable_now());
}

#[test]
#[cfg_attr(miri, ignore)]
fn wait_survives_a_spurious_poll() {
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector)
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let mut registration = IoRegistration::new(&ctx, rx.as_fd(), &Cell::new(None), Direction::Read);
    let mut wait = registration.wait();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    ctx.flush().unwrap();
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(selector.as_fd().wait_readable_blocking(1000));
    ctx.process_selector_events().unwrap();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(()));
}

#[test]
#[cfg_attr(miri, ignore)]
fn closed_and_reused_fd_number_does_not_affect_the_new_socket() {
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector)
    };
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let (old_rx, _old_tx) = socketpair();
    let mut old = IoRegistration::new(&ctx, old_rx.as_fd(), &Cell::new(None), Direction::Read);
    let mut wait = old.wait();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    ctx.flush().unwrap();
    drop(wait);
    drop(old);
    drop(old_rx);

    let (rx, tx) = socketpair();
    let mut registration = IoRegistration::new(&ctx, rx.as_fd(), &Cell::new(None), Direction::Read);
    let mut wait = registration.wait();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    ctx.flush().unwrap();
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(selector.as_fd().wait_readable_blocking(1000));
    ctx.process_selector_events().unwrap();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(()));
}

#[test]
#[cfg_attr(miri, ignore)]
fn sequential_registrations_reuse_the_slot() {
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector)
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    for _ in 0..3 {
        let mut registration =
            IoRegistration::new(&ctx, rx.as_fd(), &Cell::new(None), Direction::Read);
        let mut wait = registration.wait();
        assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
        ctx.flush().unwrap();
        rustix::io::write(&tx, b"x").unwrap();
        assert!(selector.as_fd().wait_readable_blocking(1000));
        ctx.process_selector_events().unwrap();
        assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(()));
    }
    assert_eq!(ctx.storage.io_waiters.count(), 0);
}
