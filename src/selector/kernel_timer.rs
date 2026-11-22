use crate::{const_default::ConstDefault, selector::TimerChange, time::Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum KernelTimer {
    Disarmed,
    Armed(Instant),
}

impl KernelTimer {
    pub(crate) fn flush(
        self,
        deadline: Option<Instant>,
        now: Instant,
    ) -> (Self, Option<TimerChange>) {
        match (self, deadline) {
            (Self::Armed(armed), Some(deadline)) if armed == deadline => (self, None),
            (_, Some(deadline)) => (
                Self::Armed(deadline),
                Some(TimerChange::Arm(deadline.saturating_duration_since(now))),
            ),
            (Self::Armed(_), None) => (Self::Disarmed, Some(TimerChange::Disarm)),
            (Self::Disarmed, None) => (self, None),
        }
    }
}

impl ConstDefault for KernelTimer {
    const DEFAULT: Self = Self::Disarmed;
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use crate::{
        selector::{KernelTimer, TimerChange},
        time::Instant,
    };

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn nothing_pending() {
        assert_eq!(KernelTimer::Disarmed.flush(None, Instant::now()).1, None);
    }

    #[test]
    fn arm_is_relative_to_flush_time() {
        let now = Instant::now();
        assert_eq!(
            KernelTimer::Disarmed
                .flush(Some(now.saturating_add(ms(20))), now.saturating_add(ms(5)))
                .1,
            Some(TimerChange::Arm(ms(15)))
        );
    }

    #[test]
    fn past_deadline_arms_immediately() {
        let now = Instant::now();
        assert_eq!(
            KernelTimer::Disarmed
                .flush(Some(now), now.saturating_add(ms(5)))
                .1,
            Some(TimerChange::Arm(ms(0)))
        );
    }

    #[test]
    fn same_deadline_is_not_rearmed() {
        let now = Instant::now();
        let (timer, _) = KernelTimer::Disarmed.flush(Some(now.saturating_add(ms(20))), now);
        assert_eq!(timer.flush(Some(now.saturating_add(ms(20))), now).1, None);
    }

    #[test]
    fn new_deadline_rearms() {
        let now = Instant::now();
        let (timer, _) = KernelTimer::Disarmed.flush(Some(now.saturating_add(ms(20))), now);
        assert_eq!(
            timer.flush(Some(now.saturating_add(ms(10))), now).1,
            Some(TimerChange::Arm(ms(10)))
        );
    }

    #[test]
    fn disarm_when_armed() {
        let now = Instant::now();
        let (timer, _) = KernelTimer::Disarmed.flush(Some(now.saturating_add(ms(1))), now);
        assert_eq!(timer.flush(None, now).1, Some(TimerChange::Disarm));
    }
}
