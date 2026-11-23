use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fd::AsFd,
};

pub(crate) trait FdTestingExt: AsFd {
    fn wait_readable_blocking(&self, timeout_ms: i64) -> bool {
        let fd = self.as_fd();
        let mut fds = [PollFd::new(&fd, PollFlags::IN)];
        let timeout = Timespec {
            tv_sec: timeout_ms / 1000,
            tv_nsec: timeout_ms % 1000 * 1_000_000,
        };
        poll(&mut fds, Some(&timeout)).unwrap() == 1
    }
}

impl<T: AsFd + ?Sized> FdTestingExt for T {}
