use core::time::Duration;

use crate::sys::monotonic_nanos;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[must_use]
pub(crate) struct Instant(u64);

impl Instant {
    pub(crate) const ZERO: Self = Self(0);

    pub(crate) fn now() -> Self {
        Self(monotonic_nanos())
    }

    pub(crate) fn saturating_add(self, duration: Duration) -> Self {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        Self(self.0.saturating_add(nanos))
    }

    pub(crate) const fn saturating_sub(self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}
