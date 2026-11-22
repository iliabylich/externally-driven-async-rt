mod instant;
mod timers;

#[cfg(test)]
mod tests;

pub(crate) use self::{instant::Instant, timers::Timers};
