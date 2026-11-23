use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fd::AsFd,
};

use crate::{EventLoop, EventLoopError, testing::FdTestingExt};

pub(super) trait EventLoopExt {
    fn run_until_root_finishes(&mut self);

    fn drain_until_readable(&mut self, peer: impl AsFd);

    fn peer_read_exact(&mut self, peer: impl AsFd, buf: &mut [u8]);
}

impl<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> EventLoopExt for EventLoop<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn run_until_root_finishes(&mut self) {
        loop {
            assert!(
                self.wait_readable_blocking(2000),
                "the loop never became ready"
            );
            match self.drain_events_without_blocking() {
                Ok(()) => {}
                Err(EventLoopError::RootFinished) => return,
                Err(err) => panic!("{err}"),
            }
        }
    }

    fn drain_until_readable(&mut self, peer: impl AsFd) {
        loop {
            let mut fds = [
                PollFd::new(&peer, PollFlags::IN),
                PollFd::new(&*self, PollFlags::IN),
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
            assert_eq!(self.drain_events_without_blocking(), Ok(()));
        }
    }

    fn peer_read_exact(&mut self, peer: impl AsFd, buf: &mut [u8]) {
        let mut filled = 0;
        while filled < buf.len() {
            self.drain_until_readable(&peer);
            let len = rustix::io::read(&peer, &mut buf[filled..]).unwrap();
            assert_ne!(len, 0, "the peer's socket was closed early");
            filled += len;
        }
    }
}
