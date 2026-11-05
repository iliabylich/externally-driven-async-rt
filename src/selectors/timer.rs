use core::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TimerChange {
    Arm(Duration),
    Disarm,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Timer {
    #[default]
    Disarmed,
    Armed,
    ArmPending(Duration),
    DisarmPending,
}

impl Timer {
    pub(super) fn arm_in_memory(&mut self, after: Duration) {
        *self = Self::ArmPending(round_up_to_millis(after));
    }

    pub(super) fn disarm_in_memory(&mut self) {
        *self = match self {
            Self::Disarmed => Self::Disarmed,
            Self::Armed | Self::ArmPending(_) | Self::DisarmPending => Self::DisarmPending,
        };
    }

    pub(super) fn flush(&mut self) -> Option<TimerChange> {
        match *self {
            Self::ArmPending(after) => {
                *self = Self::Armed;
                Some(TimerChange::Arm(after))
            }
            Self::DisarmPending => {
                *self = Self::Disarmed;
                Some(TimerChange::Disarm)
            }
            Self::Disarmed | Self::Armed => None,
        }
    }

    pub(super) fn fired(&mut self) {
        *self = match *self {
            Self::Armed | Self::DisarmPending | Self::Disarmed => Self::Disarmed,
            Self::ArmPending(after) => Self::ArmPending(after),
        };
    }
}

fn round_up_to_millis(d: Duration) -> Duration {
    let millis = d.as_nanos().div_ceil(1_000_000);
    Duration::from_millis(millis.try_into().unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_pending() {
        assert_eq!(Timer::default().flush(), None);
    }

    #[test]
    fn arm() {
        let mut timer = Timer::default();
        timer.arm_in_memory(Duration::from_millis(20));
        assert_eq!(
            timer.flush(),
            Some(TimerChange::Arm(Duration::from_millis(20)))
        );
    }

    #[test]
    fn disarm_when_armed() {
        let mut timer = Timer::default();
        timer.arm_in_memory(Duration::from_millis(1));
        timer.flush();
        timer.disarm_in_memory();
        assert_eq!(timer.flush(), Some(TimerChange::Disarm));
    }

    #[test]
    fn disarm_when_not_armed_is_dropped() {
        let mut timer = Timer::default();
        timer.arm_in_memory(Duration::from_millis(1));
        timer.flush();
        timer.fired();
        timer.disarm_in_memory();
        assert_eq!(timer.flush(), None);
    }
}
