use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fd::{AsFd, OwnedFd},
};

use crate::{
    Error, EventLoop, State,
    testing::{Log, TestLoop, is_readable},
};

pub(crate) fn wake_by_writing(event_loop: &impl AsFd, peer: &OwnedFd) {
    rustix::io::write(peer, b"x").unwrap();
    assert!(is_readable(event_loop, 1000));
}

pub(crate) fn run_until_root_finishes<S: State>(event_loop: &mut TestLoop<'_, S>) {
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

pub(crate) fn drain_until_readable<S: State>(event_loop: &mut TestLoop<S>, peer: impl AsFd) {
    loop {
        let mut fds = [
            PollFd::new(&peer, PollFlags::IN),
            PollFd::new(&*event_loop, PollFlags::IN),
        ];
        let timeout = Timespec {
            tv_sec: 2,
            tv_nsec: 0,
        };
        assert_ne!(
            poll(&mut fds, Some(&timeout)).unwrap(),
            0,
            "neither the peer nor the loop became ready"
        );
        if !fds[0].revents().is_empty() {
            return;
        }
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    }
}

pub(crate) fn peer_read_exact<S: State>(
    event_loop: &mut TestLoop<S>,
    peer: impl AsFd,
    buf: &mut [u8],
) {
    let mut filled = 0;
    while filled < buf.len() {
        drain_until_readable(event_loop, &peer);
        let len = rustix::io::read(&peer, &mut buf[filled..]).unwrap();
        assert_ne!(len, 0, "the peer's socket was closed early");
        filled += len;
    }
}

pub(crate) fn run_until_events<
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>(
    event_loop: &mut EventLoop<
        '_,
        Log,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    count: usize,
) {
    while event_loop.state_mut().events().len() < count {
        assert!(
            is_readable(&*event_loop, 2000),
            "the loop never became ready"
        );
        assert_eq!(event_loop.drain_events_without_blocking(), Ok(()));
    }
}
