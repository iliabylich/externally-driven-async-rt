use core::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum TimerChange {
    Arm(Duration),
    Disarm,
}
