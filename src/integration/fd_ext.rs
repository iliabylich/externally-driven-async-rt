use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fd::AsFd,
};

use crate::testing::FdTestingExt;

pub(super) trait FdExt: AsFd {
    fn wait_writable_blocking(&self, timeout_ms: i64) -> bool {
        let fd = self.as_fd();
        let mut fds = [PollFd::new(&fd, PollFlags::OUT)];
        let timeout = Timespec {
            tv_sec: timeout_ms / 1000,
            tv_nsec: timeout_ms % 1000 * 1_000_000,
        };
        poll(&mut fds, Some(&timeout)).unwrap() == 1
    }

    fn is_readable_now(&self) -> bool {
        self.wait_readable_blocking(0)
    }

    fn fill_send_buffer(&self) {
        while rustix::io::write(self.as_fd(), &[0; 4096]).is_ok() {}
    }

    fn drain_receive_buffer(&self) {
        let mut buf = [0; 4096];
        while rustix::io::read(self.as_fd(), &mut buf).is_ok() {}
    }
}

impl<T: AsFd + ?Sized> FdExt for T {}
