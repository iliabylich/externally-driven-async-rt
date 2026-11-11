use crate::{selector::TimerChange, time::Instant};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub(crate) enum KernelTimer {
    #[default]
    Disarmed,
    Armed(Instant),
    ArmPending(Instant),
    DisarmPending,
}

impl KernelTimer {
    pub(crate) fn arm_in_memory(&mut self, deadline: Instant) {
        if *self != Self::Armed(deadline) {
            *self = Self::ArmPending(deadline);
        }
    }

    pub(crate) const fn disarm_in_memory(&mut self) {
        *self = match self {
            Self::Disarmed => Self::Disarmed,
            Self::Armed(_) | Self::ArmPending(_) | Self::DisarmPending => Self::DisarmPending,
        };
    }

    pub(crate) const fn flush(&mut self, now: Instant) -> Option<TimerChange> {
        match *self {
            Self::ArmPending(deadline) => {
                *self = Self::Armed(deadline);
                Some(TimerChange::Arm(deadline.saturating_duration_since(now)))
            }
            Self::DisarmPending => {
                *self = Self::Disarmed;
                Some(TimerChange::Disarm)
            }
            Self::Disarmed | Self::Armed(_) => None,
        }
    }

    pub(crate) const fn fired(&mut self) {
        *self = match *self {
            Self::Armed(_) | Self::DisarmPending | Self::Disarmed => Self::Disarmed,
            Self::ArmPending(deadline) => Self::ArmPending(deadline),
        };
    }
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
        assert_eq!(KernelTimer::default().flush(Instant::now()), None);
    }

    #[test]
    fn arm_is_relative_to_flush_time() {
        let now = Instant::now();
        let mut timer = KernelTimer::default();
        timer.arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now + ms(5)), Some(TimerChange::Arm(ms(15))));
    }

    #[test]
    fn past_deadline_arms_immediately() {
        let now = Instant::now();
        let mut timer = KernelTimer::default();
        timer.arm_in_memory(now);
        assert_eq!(timer.flush(now + ms(5)), Some(TimerChange::Arm(ms(0))));
    }

    #[test]
    fn same_deadline_is_not_rearmed() {
        let now = Instant::now();
        let mut timer = KernelTimer::default();
        timer.arm_in_memory(now + ms(20));
        timer.flush(now);
        timer.arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now), None);
    }

    #[test]
    fn same_deadline_is_rearmed_after_firing() {
        let now = Instant::now();
        let mut timer = KernelTimer::default();
        timer.arm_in_memory(now + ms(20));
        timer.flush(now);
        timer.fired();
        timer.arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now + ms(10)), Some(TimerChange::Arm(ms(10))));
    }

    #[test]
    fn new_deadline_rearms() {
        let now = Instant::now();
        let mut timer = KernelTimer::default();
        timer.arm_in_memory(now + ms(20));
        timer.flush(now);
        timer.arm_in_memory(now + ms(10));
        assert_eq!(timer.flush(now), Some(TimerChange::Arm(ms(10))));
    }

    #[test]
    fn disarm_when_armed() {
        let now = Instant::now();
        let mut timer = KernelTimer::default();
        timer.arm_in_memory(now + ms(1));
        timer.flush(now);
        timer.disarm_in_memory();
        assert_eq!(timer.flush(now), Some(TimerChange::Disarm));
    }

    #[test]
    fn disarm_when_not_armed_is_dropped() {
        let now = Instant::now();
        let mut timer = KernelTimer::default();
        timer.arm_in_memory(now + ms(1));
        timer.flush(now);
        timer.fired();
        timer.disarm_in_memory();
        assert_eq!(timer.flush(now), None);
    }

    #[test]
    fn firing_keeps_a_pending_new_deadline() {
        let now = Instant::now();
        let mut timer = KernelTimer::default();
        timer.arm_in_memory(now + ms(10));
        timer.flush(now);
        timer.arm_in_memory(now + ms(20));
        timer.fired();
        assert_eq!(timer.flush(now), Some(TimerChange::Arm(ms(20))));
    }
}
