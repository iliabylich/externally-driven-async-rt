use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fd::{AsFd, OwnedFd};
use rustix::net::{AddressFamily, SocketFlags, SocketType, socketpair};

use crate::io::IoRegistration;
use crate::selectors::Direction;
use crate::{Context, Error, EventLoop, State};

pub(crate) const TEST_MAX_TASKS: usize = 128;

pub(crate) const TEST_MAX_TIMERS: usize = 128;

pub(crate) const TEST_MAX_IO: usize = 64;

pub(crate) type TestContext = Context<Log>;

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

pub(crate) fn readable<'a>(
    ctx: &'a TestContext,
    fd: &'a OwnedFd,
) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(async move {
        let mut registration = IoRegistration::new(ctx.event_loop(), fd.as_fd(), Direction::Read);
        while !is_readable(fd, 0) {
            registration.wait().await;
        }
    })
}

pub(crate) fn wake_by_writing(event_loop: &impl AsFd, peer: &OwnedFd) {
    rustix::io::write(peer, b"x").unwrap();
    assert!(is_readable(event_loop, 1000));
}

pub(crate) fn run_until_root_finishes<S: State>(event_loop: &mut Pin<Box<EventLoop<S>>>) {
    loop {
        assert!(
            is_readable(&**event_loop, 2000),
            "the loop never became ready"
        );
        match event_loop.as_mut().drain_events_without_blocking() {
            Ok(()) => {}
            Err(Error::RootFinished) => return,
            Err(err) => panic!("{err}"),
        }
    }
}

pub(crate) fn has_timers<S: State>(event_loop: &EventLoop<S>) -> bool {
    event_loop
        .storage()
        .borrow()
        .next_timer_deadline()
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

pub(crate) fn run_until_events(event_loop: &mut Pin<Box<EventLoop<Log>>>, count: usize) {
    while event_loop.as_mut().state_mut().0.len() < count {
        assert!(
            is_readable(&**event_loop, 2000),
            "the loop never became ready"
        );
        assert_eq!(event_loop.as_mut().drain_events_without_blocking(), Ok(()));
    }
}
