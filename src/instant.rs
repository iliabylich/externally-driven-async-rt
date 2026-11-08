use core::{ops::Add, time::Duration};

use crate::selectors::monotonic_nanos;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Instant(u64);

impl Instant {
    pub(crate) const ZERO: Self = Self(0);

    pub(crate) fn now() -> Self {
        Self(monotonic_nanos())
    }

    pub(crate) fn saturating_duration_since(self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

impl Add<Duration> for Instant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        Self(self.0.saturating_add(nanos))
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use crate::{instant::Instant, test_utils::sleep};

    #[test]
    fn now_is_monotonic() {
        let earlier = Instant::now();
        assert!(Instant::now() >= earlier);
    }

    #[test]
    fn adding_and_measuring_round_trip() {
        let now = Instant::now();
        let later = now + Duration::from_millis(5);
        assert_eq!(
            later.saturating_duration_since(now),
            Duration::from_millis(5)
        );
        assert_eq!(now.saturating_duration_since(later), Duration::ZERO);
    }

    #[test]
    fn adding_saturates() {
        let now = Instant::now();
        assert_eq!(now + Duration::MAX, Instant(u64::MAX));
    }

    #[test]
    fn advances_at_least_as_long_as_a_sleep() {
        let start = Instant::now();
        sleep(Duration::from_millis(20));
        assert!(Instant::now().saturating_duration_since(start) >= Duration::from_millis(20));
    }
}
