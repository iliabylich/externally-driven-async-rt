use core::{
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::Duration,
};

use crate::time::{Instant, Timers};

pub(crate) fn fake_instant(millis: &AtomicU64) -> Instant {
    Instant(0).saturating_add(Duration::from_millis(millis.load(Relaxed)))
}

pub(crate) fn wake_expired_timers<const TIMERS: usize>(timers: &Timers<TIMERS>) {
    let now = timers.now();
    while let Some(waker) = timers.take_expired(now) {
        waker.wake();
    }
}
