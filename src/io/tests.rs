use core::{
    cell::Cell,
    cell::RefCell,
    future::{Future, pending, poll_fn},
    pin::{Pin, pin},
    task::{Poll, Waker},
};

use rustix::{
    fd::{AsFd, OwnedFd},
    io::Errno,
};

use crate::{
    Context, LoopStorage, Selector, State,
    io::{IoIndex, IoRegistration},
    selector::Direction,
    testing::{
        EventLoopTestingExt, Log, TestContext, TestLoop, TestStorage, is_readable, socketpair,
    },
};

async fn read_some<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
>(
    ctx: &Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    fd: &OwnedFd,
    cached: &Cell<Option<IoIndex>>,
) -> rustix::io::Result<usize> {
    let mut buf = [0; 64];
    let mut registration = IoRegistration::new(ctx.storage(), fd.as_fd(), cached, Direction::Read);
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
    let mut registration = IoRegistration::new(ctx.storage(), fd.as_fd(), cached, Direction::Write);
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

fn registrations(event_loop: &TestLoop<Log>) -> usize {
    event_loop.storage().io_waiters().count()
}

#[test]
#[cfg_attr(miri, ignore)]
fn read_waits_until_the_peer_writes() {
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let n = read_some(&ctx, &rx, &Cell::new(None)).await.unwrap();
            ctx.dispatch(n as u32);
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(registrations(&event_loop), 1);
    assert!(!is_readable(&event_loop, 0));

    rustix::io::write(&tx, b"abc").unwrap();
    assert!(is_readable(&event_loop, 1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_eq!(event_loop.state_mut().events(), [3]);
    assert_eq!(registrations(&event_loop), 0);
}

#[test]
#[cfg_attr(miri, ignore)]
fn write_waits_on_a_full_buffer() {
    let (tx, rx) = socketpair();
    fill_send_buffer(&tx);
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let n = write_some(&ctx, &tx, &Cell::new(None)).await.unwrap();
            ctx.dispatch(n as u32);
        },
    )
    .unwrap();
    assert!(event_loop.state_mut().events().is_empty());
    assert!(!is_readable(&event_loop, 0));

    drain_receive_buffer(&rx);
    event_loop.run_until_root_finishes();
    assert_eq!(event_loop.state_mut().events(), [1]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn read_and_write_wait_on_one_fd_at_the_same_time() {
    let (a, b) = socketpair();
    fill_send_buffer(&a);
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let cached = Cell::new(None);
            let mut read = pin!(read_some(&ctx, &a, &cached));
            let mut write = pin!(write_some(&ctx, &a, &cached));
            let (mut read_done, mut write_done) = (false, false);
            poll_fn(|cx| {
                if !read_done && let Poll::Ready(n) = read.as_mut().poll(cx) {
                    ctx.dispatch(n.unwrap() as u32);
                    read_done = true;
                }
                if !write_done && let Poll::Ready(n) = write.as_mut().poll(cx) {
                    ctx.dispatch(100 + n.unwrap() as u32);
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
    assert_eq!(registrations(&event_loop), 1);
    assert!(!is_readable(&event_loop, 0));

    rustix::io::write(&b, b"abc").unwrap();
    assert!(is_readable(&event_loop, 1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_eq!(event_loop.state_mut().events(), [3]);

    drain_receive_buffer(&b);
    assert!(is_readable(&event_loop, 1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_eq!(event_loop.state_mut().events(), [3, 101]);
}

type IoStorage = LoopStorage<1, 64, 0, 1, 1>;

#[test]
#[cfg_attr(miri, ignore)]
fn dropping_a_pending_wait_deregisters() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(Log::default());
    let storage = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let mut registration =
        IoRegistration::new(&storage, rx.as_fd(), &Cell::new(None), Direction::Read);
    let mut wait = registration.wait();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    storage.flush().unwrap();
    drop(wait);
    drop(registration);
    assert_eq!(storage.io_waiters().count(), 0);
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(!is_readable(selector.as_fd(), 0));
}

#[test]
#[cfg_attr(miri, ignore)]
fn wait_survives_a_spurious_poll() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(Log::default());
    let storage = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let mut registration =
        IoRegistration::new(&storage, rx.as_fd(), &Cell::new(None), Direction::Read);
    let mut wait = registration.wait();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    storage.flush().unwrap();
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(is_readable(selector.as_fd(), 1000));
    storage.process_selector_events().unwrap();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(()));
}

#[test]
#[cfg_attr(miri, ignore)]
fn closed_and_reused_fd_number_does_not_affect_the_new_socket() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(Log::default());
    let storage = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let (old_rx, _old_tx) = socketpair();
    let mut old = IoRegistration::new(&storage, old_rx.as_fd(), &Cell::new(None), Direction::Read);
    let mut wait = old.wait();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    storage.flush().unwrap();
    drop(wait);
    drop(old);
    drop(old_rx);

    let (rx, tx) = socketpair();
    let mut registration =
        IoRegistration::new(&storage, rx.as_fd(), &Cell::new(None), Direction::Read);
    let mut wait = registration.wait();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    storage.flush().unwrap();
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(is_readable(selector.as_fd(), 1000));
    storage.process_selector_events().unwrap();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(()));
}

#[test]
#[cfg_attr(miri, ignore)]
fn sequential_registrations_reuse_the_slot() {
    let selector = Selector::new().unwrap();
    let state = RefCell::new(Log::default());
    let storage = {
        static STORAGE: IoStorage = IoStorage::new();
        STORAGE.claim(&selector, &state)
    };
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    for _ in 0..3 {
        let mut registration =
            IoRegistration::new(&storage, rx.as_fd(), &Cell::new(None), Direction::Read);
        let mut wait = registration.wait();
        assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
        storage.flush().unwrap();
        rustix::io::write(&tx, b"x").unwrap();
        assert!(is_readable(selector.as_fd(), 1000));
        storage.process_selector_events().unwrap();
        assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(()));
    }
    assert_eq!(storage.io_waiters().count(), 0);
}
