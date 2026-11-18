use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fd::{AsFd, BorrowedFd},
};

pub(crate) trait FdTestingExt: AsFd {
    fn wait_readable_blocking(&self, timeout_ms: i64) -> bool {
        is_ready(self.as_fd(), PollFlags::IN, timeout_ms)
    }

    fn wait_writable_blocking(&self, timeout_ms: i64) -> bool {
        is_ready(self.as_fd(), PollFlags::OUT, timeout_ms)
    }

    fn is_readable_now(&self) -> bool {
        self.wait_readable_blocking(0)
    }
}

impl<T: AsFd + ?Sized> FdTestingExt for T {}

fn is_ready(fd: BorrowedFd<'_>, flags: PollFlags, timeout_ms: i64) -> bool {
    let mut fds = [PollFd::new(&fd, flags)];
    let timeout = Timespec {
        tv_sec: timeout_ms / 1000,
        tv_nsec: timeout_ms % 1000 * 1_000_000,
    };
    poll(&mut fds, Some(&timeout)).unwrap() == 1
}
