use core::{cell::Cell, time::Duration};

use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fd::{AsFd, OwnedFd},
    thread::{NanosleepRelativeResult, nanosleep},
};

use crate::{io::IoRegistration, selector::Direction, testing::TestContext};

pub(crate) fn is_readable(fd: impl AsFd, timeout_ms: i64) -> bool {
    is_ready(fd, PollFlags::IN, timeout_ms)
}

pub(crate) fn is_writable(fd: impl AsFd, timeout_ms: i64) -> bool {
    is_ready(fd, PollFlags::OUT, timeout_ms)
}

fn is_ready(fd: impl AsFd, flags: PollFlags, timeout_ms: i64) -> bool {
    let mut fds = [PollFd::new(&fd, flags)];
    let timeout = Timespec {
        tv_sec: timeout_ms / 1000,
        tv_nsec: timeout_ms % 1000 * 1_000_000,
    };
    poll(&mut fds, Some(&timeout)).unwrap() == 1
}

pub(crate) fn sleep_blocking(duration: Duration) {
    let timeout = Timespec {
        tv_sec: duration.as_secs() as i64,
        tv_nsec: i64::from(duration.subsec_nanos()),
    };
    assert!(matches!(nanosleep(&timeout), NanosleepRelativeResult::Ok));
}

pub(crate) async fn wait_readable(ctx: &TestContext<'_>, fd: &OwnedFd) {
    let mut registration =
        IoRegistration::new(ctx.storage(), fd.as_fd(), &Cell::new(None), Direction::Read);
    while !is_readable(fd, 0) {
        registration.wait().await;
    }
}
