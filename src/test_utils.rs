use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fd::{AsFd, BorrowedFd, OwnedFd};
use rustix::net::{AddressFamily, SocketFlags, SocketType, socketpair};

use crate::inner::Direction;
use crate::selectors::Token;
use crate::{Context, Error, EventLoop, State};

#[derive(Default)]
pub(crate) struct Log(pub(crate) Vec<u32>);

impl State for Log {
    type Event = u32;

    fn apply(&mut self, event: u32) {
        self.0.push(event);
    }
}

pub(crate) fn pair() -> (OwnedFd, OwnedFd) {
    let (a, b) = socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::empty(),
        None,
    )
    .unwrap();
    rustix::io::ioctl_fionbio(&a, true).unwrap();
    rustix::io::ioctl_fionbio(&b, true).unwrap();
    (a, b)
}

pub(crate) fn is_readable(fd: impl AsFd, timeout_ms: i64) -> bool {
    let mut fds = [PollFd::new(&fd, PollFlags::IN)];
    let timeout = Timespec {
        tv_sec: timeout_ms / 1000,
        tv_nsec: timeout_ms % 1000 * 1_000_000,
    };
    poll(&mut fds, Some(&timeout)).unwrap() == 1
}

// Stand-in for real IO futures (step 11): resolves once `fd` is readable.
pub(crate) struct Readable<'a> {
    ctx: &'a Context<u32>,
    fd: BorrowedFd<'a>,
    token: Option<Token>,
}

pub(crate) fn readable<'a>(ctx: &'a Context<u32>, fd: &'a OwnedFd) -> Readable<'a> {
    Readable {
        ctx,
        fd: fd.as_fd(),
        token: None,
    }
}

impl Future for Readable<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        if is_readable(self.fd, 0) {
            return Poll::Ready(());
        }
        let inner = self.ctx.inner();
        let token = match self.token {
            Some(token) => token,
            None => inner.register_io(self.fd),
        };
        inner.set_io_waker(token, Direction::Read, cx.waker());
        self.token = Some(token);
        Poll::Pending
    }
}

impl Drop for Readable<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.token {
            self.ctx.inner().deregister_io(token).unwrap();
        }
    }
}

pub(crate) fn wake_by_writing(event_loop: &impl AsFd, peer: &OwnedFd) {
    rustix::io::write(peer, b"x").unwrap();
    assert!(is_readable(event_loop, 1000));
}

pub(crate) fn run_until_root_finishes<S: State>(event_loop: &mut EventLoop<S>) {
    loop {
        assert!(
            is_readable(&*event_loop, 2000),
            "the loop never became ready"
        );
        match event_loop.drain_events_without_blocking() {
            Ok(()) => {}
            Err(Error::RootFinished) => return,
            Err(err) => panic!("{err}"),
        }
    }
}

pub(crate) fn has_timers<S: State>(event_loop: &EventLoop<S>) -> bool {
    event_loop
        .inner()
        .timers()
        .borrow()
        .next_deadline()
        .is_some()
}

// Resolves with whichever future completes first, dropping the other one.
pub(crate) struct First<A, B>(pub(crate) A, pub(crate) B);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Winner {
    A,
    B,
}

impl<A: Future + Unpin, B: Future + Unpin> Future for First<A, B> {
    type Output = Winner;

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Winner> {
        if Pin::new(&mut self.0).poll(cx).is_ready() {
            return Poll::Ready(Winner::A);
        }
        if Pin::new(&mut self.1).poll(cx).is_ready() {
            return Poll::Ready(Winner::B);
        }
        Poll::Pending
    }
}
