/// An error returned from [`Sleep`](crate::Sleep) and
/// [`Context::timeout`](crate::Context::timeout) when all `TIMERS` timer slots are taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimersFullError;

impl core::fmt::Display for TimersFullError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("all timer slots are taken")
    }
}

impl core::error::Error for TimersFullError {}
