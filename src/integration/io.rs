use core::{
    cell::Cell,
    future::{Future, pending, poll_fn},
    pin::pin,
    task::{Context, Poll},
};

use rustix::fd::{AsFd, OwnedFd};

use crate::{
    EventLoop, LoopStorage, NotEnoughResources, Selector,
    io::IoWaiter,
    net::NetError,
    selector::Direction,
    testing::{EventLoopTestingExt, assert_yields, counting_waker},
};

use super::{EventLoopExt, FdExt, socketpair};

type Storage<E> = LoopStorage<E, 1, 1, 0, 1024, 1, 1>;

async fn read_some(io: &IoWaiter, fd: &OwnedFd) -> rustix::io::Result<usize> {
    let mut buf = [0; 64];
    io.retry(Direction::Read, || rustix::io::read(fd, &mut buf))
        .await
}

async fn write_some(io: &IoWaiter, fd: &OwnedFd) -> rustix::io::Result<usize> {
    io.retry(Direction::Write, || rustix::io::write(fd, b"x"))
        .await
}

#[test]
fn data_written_before_registration_is_read_right_away() {
    static STORAGE: Storage<u32> = Storage::new();
    let (rx, tx) = socketpair();
    rustix::io::write(&tx, b"abc").unwrap();
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let io_waiters = &ctx.storage.io_waiters;
        let io = io_waiters.register(ctx.selector, rx.as_fd()).unwrap();
        let n = read_some(io, &rx).await.unwrap();
        io_waiters.deregister(ctx.selector, rx.as_fd());
        ctx.dispatch(u32::try_from(n).unwrap()).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), [3]);
}

#[test]
fn read_waits_until_the_peer_writes() {
    static STORAGE: Storage<u32> = Storage::new();
    let (rx, tx) = socketpair();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let io_waiters = &ctx.storage.io_waiters;
        let io = io_waiters.register(ctx.selector, rx.as_fd()).unwrap();
        let n = read_some(io, &rx).await.unwrap();
        io_waiters.deregister(ctx.selector, rx.as_fd());
        ctx.dispatch(u32::try_from(n).unwrap()).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), []);

    rustix::io::write(&tx, b"abc").unwrap();
    assert_eq!(event_loop.run_until_events::<1>(), [3]);
}

#[test]
fn write_waits_on_a_full_buffer() {
    static STORAGE: Storage<u32> = Storage::new();
    let (tx, rx) = socketpair();
    tx.fill_send_buffer();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let io_waiters = &ctx.storage.io_waiters;
        let io = io_waiters.register(ctx.selector, tx.as_fd()).unwrap();
        let n = write_some(io, &tx).await.unwrap();
        io_waiters.deregister(ctx.selector, tx.as_fd());
        ctx.dispatch(u32::try_from(n).unwrap()).unwrap();
    })
    .unwrap();
    assert_yields!(event_loop.events(), []);

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
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let io_waiters = &ctx.storage.io_waiters;
        let io = io_waiters.register(ctx.selector, a.as_fd()).unwrap();
        let mut read = pin!(read_some(io, &a));
        let mut write = pin!(write_some(io, &a));
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
        io_waiters.deregister(ctx.selector, a.as_fd());
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), []);

    rustix::io::write(&b, b"abc").unwrap();
    assert_eq!(event_loop.run_until_events::<1>(), [3]);

    b.drain_receive_buffer();
    assert_eq!(event_loop.run_until_events::<1>(), [101]);
}

#[test]
fn registering_more_than_fds_fds_fails_until_one_is_deregistered() {
    static STORAGE: Storage<u32> = Storage::new();
    let (a, b) = socketpair();
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let io_waiters = &ctx.storage.io_waiters;
        io_waiters.register(ctx.selector, a.as_fd()).unwrap();
        assert_eq!(
            io_waiters.register(ctx.selector, b.as_fd()).err(),
            Some(NetError::NotEnoughResources(NotEnoughResources::FDs))
        );
        io_waiters.deregister(ctx.selector, a.as_fd());
        io_waiters.register(ctx.selector, b.as_fd()).unwrap();
        io_waiters.deregister(ctx.selector, b.as_fd());
        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), [1]);
}

#[test]
fn closed_and_reused_fd_number_does_not_affect_the_new_socket() {
    static STORAGE: Storage<()> = Storage::new();
    let new_tx = Cell::new(None);
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let io_waiters = &ctx.storage.io_waiters;
        let (old_rx, _old_tx) = socketpair();
        io_waiters.register(ctx.selector, old_rx.as_fd()).unwrap();
        io_waiters.deregister(ctx.selector, old_rx.as_fd());
        drop(old_rx);

        let (rx, tx) = socketpair();
        new_tx.set(Some(tx));
        let io = io_waiters.register(ctx.selector, rx.as_fd()).unwrap();
        read_some(io, &rx).await.unwrap();
        io_waiters.deregister(ctx.selector, rx.as_fd());
        ctx.dispatch(()).unwrap();
    })
    .unwrap();
    let tx = new_tx.take().unwrap();
    rustix::io::write(&tx, b"abc").unwrap();
    event_loop.run_until_root_finishes();
    assert_yields!(event_loop.events(), [()]);
}

#[test]
fn clearing_drops_io_wakers() {
    static STORAGE: Storage<()> = Storage::new();
    let drops = Cell::new(0);
    let selector = Selector::new().unwrap();
    let ctx = STORAGE.claim(&selector).unwrap().0;
    let (fd, _peer) = socketpair();
    let io = ctx
        .storage
        .io_waiters
        .register(ctx.selector, fd.as_fd())
        .unwrap();
    io.clear(Direction::Read);
    let waker = counting_waker(&drops);
    let mut ready = pin!(io.ready(Direction::Read));
    assert_eq!(
        ready.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    );
    drop(waker);
    assert_eq!(drops.get(), 1);
    ctx.clear();
    assert_eq!(drops.get(), 2);
    ctx.storage.io_waiters.deregister(ctx.selector, fd.as_fd());
}
