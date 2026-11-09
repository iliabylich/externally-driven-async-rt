use core::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimerChange {
    Arm(Duration),
    Disarm,
}
