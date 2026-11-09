use core::time::Duration;

use crate::instant::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimerChange {
    Arm(Duration),
    Disarm,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Timer {
    #[default]
    Disarmed,
    Armed(Instant),
    ArmPending(Instant),
    DisarmPending,
}

impl Timer {
    pub(crate) fn arm_in_memory(&mut self, deadline: Instant) {
        if *self != Self::Armed(deadline) {
            *self = Self::ArmPending(deadline);
        }
    }

    pub(crate) fn disarm_in_memory(&mut self) {
        *self = match self {
            Self::Disarmed => Self::Disarmed,
            Self::Armed(_) | Self::ArmPending(_) | Self::DisarmPending => Self::DisarmPending,
        };
    }

    pub(crate) fn flush(&mut self, now: Instant) -> Option<TimerChange> {
        match *self {
            Self::ArmPending(deadline) => {
                *self = Self::Armed(deadline);
                Some(TimerChange::Arm(round_up_to_millis(
                    deadline.saturating_duration_since(now),
                )))
            }
            Self::DisarmPending => {
                *self = Self::Disarmed;
                Some(TimerChange::Disarm)
            }
            Self::Disarmed | Self::Armed(_) => None,
        }
    }

    pub(crate) fn fired(&mut self) {
        *self = match *self {
            Self::Armed(_) | Self::DisarmPending | Self::Disarmed => Self::Disarmed,
            Self::ArmPending(deadline) => Self::ArmPending(deadline),
        };
    }
}

fn round_up_to_millis(d: Duration) -> Duration {
    let millis = d.as_nanos().div_ceil(1_000_000);
    Duration::from_millis(millis.try_into().unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use crate::{
        instant::Instant,
        selectors::timer::{Timer, TimerChange},
    };

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn nothing_pending() {
        assert_eq!(Timer::default().flush(Instant::now()), None);
    }

    #[test]
    fn arm_is_relative_to_flush_time() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now + ms(5)), Some(TimerChange::Arm(ms(15))));
    }

    #[test]
    fn arm_rounds_up_to_whole_millis() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now + Duration::from_micros(1500));
        assert_eq!(timer.flush(now), Some(TimerChange::Arm(ms(2))));
    }

    #[test]
    fn past_deadline_arms_immediately() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now);
        assert_eq!(timer.flush(now + ms(5)), Some(TimerChange::Arm(ms(0))));
    }

    #[test]
    fn same_deadline_is_not_rearmed() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now + ms(20));
        timer.flush(now);
        timer.arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now), None);
    }

    #[test]
    fn same_deadline_is_rearmed_after_firing() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now + ms(20));
        timer.flush(now);
        timer.fired();
        timer.arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now + ms(10)), Some(TimerChange::Arm(ms(10))));
    }

    #[test]
    fn new_deadline_rearms() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now + ms(20));
        timer.flush(now);
        timer.arm_in_memory(now + ms(10));
        assert_eq!(timer.flush(now), Some(TimerChange::Arm(ms(10))));
    }

    #[test]
    fn disarm_when_armed() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now + ms(1));
        timer.flush(now);
        timer.disarm_in_memory();
        assert_eq!(timer.flush(now), Some(TimerChange::Disarm));
    }

    #[test]
    fn disarm_when_not_armed_is_dropped() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now + ms(1));
        timer.flush(now);
        timer.fired();
        timer.disarm_in_memory();
        assert_eq!(timer.flush(now), None);
    }

    #[test]
    fn firing_keeps_a_pending_new_deadline() {
        let now = Instant::now();
        let mut timer = Timer::default();
        timer.arm_in_memory(now + ms(10));
        timer.flush(now);
        timer.arm_in_memory(now + ms(20));
        timer.fired();
        assert_eq!(timer.flush(now), Some(TimerChange::Arm(ms(20))));
    }
}
