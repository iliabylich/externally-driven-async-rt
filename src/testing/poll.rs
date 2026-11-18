use core::{cell::Cell, time::Duration};

use rustix::{
    event::Timespec,
    fd::{AsFd, OwnedFd},
    thread::{NanosleepRelativeResult, nanosleep},
};

use crate::{
    io::IoRegistration,
    selector::Direction,
    testing::{FdTestingExt, TestContext},
};

pub(crate) fn sleep_blocking(duration: Duration) {
    let timeout = Timespec {
        tv_sec: duration.as_secs() as i64,
        tv_nsec: i64::from(duration.subsec_nanos()),
    };
    assert!(matches!(nanosleep(&timeout), NanosleepRelativeResult::Ok));
}

pub(crate) async fn wait_readable(ctx: &TestContext<'_>, fd: &OwnedFd) {
    let mut registration =
        IoRegistration::new(ctx, fd.as_fd(), &Cell::new(None), Direction::Read).unwrap();
    while !fd.is_readable_now() {
        registration.wait().await;
    }
}
