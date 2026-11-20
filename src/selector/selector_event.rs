use rustix::fd::RawFd;

use crate::selector::Directions;

pub(crate) const EVENTS_PER_CALL: usize = if cfg!(test) { 4 } else { 64 };

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum SelectorEvent {
    Io { fd: RawFd, ready: Directions },
    Timer,
    Notify,
}
