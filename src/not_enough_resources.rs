/// An error returned if statically allocated resources are not enough.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotEnoughResources {
    /// Not enough TASKS
    Tasks,
    /// Not enough TIMERS
    Timers,
    /// Not enough FDS
    FDs,
}

impl core::fmt::Display for NotEnoughResources {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Tasks => f.write_str("all task slots are taken"),
            Self::Timers => f.write_str("all timer slots are taken"),
            Self::FDs => f.write_str("all fd slots are taken"),
        }
    }
}

impl core::error::Error for NotEnoughResources {}
