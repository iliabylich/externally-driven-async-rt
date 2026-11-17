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
    pub(crate) fn arm_in_memory(self, deadline: Instant) -> Self {
        if self == Self::Armed(deadline) {
            self
        } else {
            Self::ArmPending(deadline)
        }
    }

    pub(crate) const fn disarm_in_memory(self) -> Self {
        match self {
            Self::Disarmed => Self::Disarmed,
            Self::Armed(_) | Self::ArmPending(_) | Self::DisarmPending => Self::DisarmPending,
        }
    }

    pub(crate) const fn flush(self, now: Instant) -> (Self, Option<TimerChange>) {
        match self {
            Self::ArmPending(deadline) => (
                Self::Armed(deadline),
                Some(TimerChange::Arm(deadline.saturating_duration_since(now))),
            ),
            Self::DisarmPending => (Self::Disarmed, Some(TimerChange::Disarm)),
            Self::Disarmed | Self::Armed(_) => (self, None),
        }
    }

    pub(crate) const fn fired(self) -> Self {
        match self {
            Self::Armed(_) | Self::DisarmPending | Self::Disarmed => Self::Disarmed,
            Self::ArmPending(deadline) => Self::ArmPending(deadline),
        }
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
        assert_eq!(KernelTimer::default().flush(Instant::now()).1, None);
    }

    #[test]
    fn arm_is_relative_to_flush_time() {
        let now = Instant::now();
        let timer = KernelTimer::default().arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now + ms(5)).1, Some(TimerChange::Arm(ms(15))));
    }

    #[test]
    fn past_deadline_arms_immediately() {
        let now = Instant::now();
        let timer = KernelTimer::default().arm_in_memory(now);
        assert_eq!(timer.flush(now + ms(5)).1, Some(TimerChange::Arm(ms(0))));
    }

    #[test]
    fn same_deadline_is_not_rearmed() {
        let now = Instant::now();
        let timer = KernelTimer::default().arm_in_memory(now + ms(20));
        let (timer, _) = timer.flush(now);
        let timer = timer.arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now).1, None);
    }

    #[test]
    fn same_deadline_is_rearmed_after_firing() {
        let now = Instant::now();
        let timer = KernelTimer::default().arm_in_memory(now + ms(20));
        let (timer, _) = timer.flush(now);
        let timer = timer.fired();
        let timer = timer.arm_in_memory(now + ms(20));
        assert_eq!(timer.flush(now + ms(10)).1, Some(TimerChange::Arm(ms(10))));
    }

    #[test]
    fn new_deadline_rearms() {
        let now = Instant::now();
        let timer = KernelTimer::default().arm_in_memory(now + ms(20));
        let (timer, _) = timer.flush(now);
        let timer = timer.arm_in_memory(now + ms(10));
        assert_eq!(timer.flush(now).1, Some(TimerChange::Arm(ms(10))));
    }

    #[test]
    fn disarm_when_armed() {
        let now = Instant::now();
        let timer = KernelTimer::default().arm_in_memory(now + ms(1));
        let (timer, _) = timer.flush(now);
        let timer = timer.disarm_in_memory();
        assert_eq!(timer.flush(now).1, Some(TimerChange::Disarm));
    }

    #[test]
    fn disarm_when_not_armed_is_dropped() {
        let now = Instant::now();
        let timer = KernelTimer::default().arm_in_memory(now + ms(1));
        let (timer, _) = timer.flush(now);
        let timer = timer.fired();
        let timer = timer.disarm_in_memory();
        assert_eq!(timer.flush(now).1, None);
    }

    #[test]
    fn firing_keeps_a_pending_new_deadline() {
        let now = Instant::now();
        let timer = KernelTimer::default().arm_in_memory(now + ms(10));
        let (timer, _) = timer.flush(now);
        let timer = timer.arm_in_memory(now + ms(20));
        let timer = timer.fired();
        assert_eq!(timer.flush(now).1, Some(TimerChange::Arm(ms(20))));
    }
}
