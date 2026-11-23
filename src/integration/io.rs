use core::{
    future::{Future, pending, poll_fn},
    pin::pin,
    task::{Poll, Waker},
};

use rustix::fd::{AsFd, OwnedFd};

use crate::{
    Context, EventLoop, LoopStorage, Selector,
    selector::Direction,
    testing::{FdTestingExt, assert_yields},
};

use super::{EventLoopExt, FdExt, socketpair};

type Storage<E> = LoopStorage<E, 1, 1, 0, 1024, 1, 1>;

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
    ctx.storage
        .io_waiters
        .retry(ctx.selector, fd.as_fd(), Direction::Read, || {
            rustix::io::read(fd, &mut buf)
        })
        .await
        .unwrap()
}

async fn write_some<
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
    ctx.storage
        .io_waiters
        .retry(ctx.selector, fd.as_fd(), Direction::Write, || {
            rustix::io::write(fd, b"x")
        })
        .await
        .unwrap()
}

#[test]
fn read_waits_until_the_peer_writes() {
    static STORAGE: Storage<u32> = Storage::new();
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        let n = read_some(&ctx, &rx).await.unwrap();
        ctx.dispatch(u32::try_from(n).unwrap()).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert!(!event_loop.is_readable_now());

    rustix::io::write(&tx, b"abc").unwrap();
    assert!(event_loop.wait_readable_blocking(1000));
    assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    assert_yields!(event_loop.events(), [3]);
}

#[test]
fn write_waits_on_a_full_buffer() {
    static STORAGE: Storage<u32> = Storage::new();
    let (tx, rx) = socketpair();
    tx.fill_send_buffer();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        let n = write_some(&ctx, &tx).await.unwrap();
        ctx.dispatch(u32::try_from(n).unwrap()).unwrap();
    })
    .unwrap();
    assert_yields!(event_loop.events(), []);
    assert!(!event_loop.is_readable_now());

    rx.drain_receive_buffer();
    event_loop.run_until_root_finishes();
    assert_yields!(event_loop.events(), [1]);
}

#[test]
fn read_and_write_wait_on_one_fd_at_the_same_time() {
    static STORAGE: Storage<u32> = Storage::new();
    let (a, b) = socketpair();
    a.fill_send_buffer();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
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
    })
    .unwrap();
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

#[test]
fn dropping_a_pending_wait_deregisters() {
    static STORAGE: Storage<()> = Storage::new();
    let selector = Selector::new().unwrap();
    let ctx = STORAGE.claim(&selector).unwrap().0;
    let (rx, tx) = socketpair();
    let mut cx = core::task::Context::from_waker(Waker::noop());
    {
        let mut ready = pin!(ctx.storage.io_waiters.ready(
            ctx.selector,
            rx.as_fd(),
            Direction::Read
        ));
        assert_eq!(ready.as_mut().poll(&mut cx), Poll::Pending);
        ctx.flush().unwrap();
    }
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(!selector.as_fd().is_readable_now());
}

#[test]
fn closed_and_reused_fd_number_does_not_affect_the_new_socket() {
    static STORAGE: Storage<()> = Storage::new();
    let selector = Selector::new().unwrap();
    let ctx = STORAGE.claim(&selector).unwrap().0;
    let mut cx = core::task::Context::from_waker(Waker::noop());
    let (old_rx, _old_tx) = socketpair();
    {
        let mut old = pin!(ctx.storage.io_waiters.ready(
            ctx.selector,
            old_rx.as_fd(),
            Direction::Read
        ));
        assert_eq!(old.as_mut().poll(&mut cx), Poll::Pending);
        ctx.flush().unwrap();
    }
    drop(old_rx);

    let (rx, tx) = socketpair();
    let mut ready = pin!(
        ctx.storage
            .io_waiters
            .ready(ctx.selector, rx.as_fd(), Direction::Read)
    );
    assert_eq!(ready.as_mut().poll(&mut cx), Poll::Pending);
    ctx.flush().unwrap();
    rustix::io::write(&tx, b"abc").unwrap();
    assert!(selector.as_fd().wait_readable_blocking(1000));
    ctx.process_selector_events().unwrap();
    assert_eq!(ready.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
}
