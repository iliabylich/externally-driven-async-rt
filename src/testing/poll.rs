use core::time::Duration;

use rustix::{
    event::Timespec,
    thread::{NanosleepRelativeResult, nanosleep},
};

use crate::time::{Instant, Timers};

pub(crate) fn sleep_blocking(duration: Duration) {
    let timeout = Timespec {
        tv_sec: i64::try_from(duration.as_secs()).unwrap(),
        tv_nsec: i64::from(duration.subsec_nanos()),
    };
    assert!(matches!(nanosleep(&timeout), NanosleepRelativeResult::Ok));
}

pub(crate) fn wake_expired_timers<const TIMERS: usize>(timers: &Timers<TIMERS>) {
    let now = Instant::now();
    while let Some(waker) = timers.take_expired(now) {
        waker.wake();
    }
}
