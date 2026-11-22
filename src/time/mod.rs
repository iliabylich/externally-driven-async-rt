mod instant;
mod timers;
mod timers_full_error;

#[cfg(test)]
mod tests;

pub use self::timers_full_error::TimersFullError;
pub(crate) use self::{instant::Instant, timers::Timers};
