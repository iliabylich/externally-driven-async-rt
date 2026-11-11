use rustix::fd::RawFd;

use crate::selector::Directions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) struct IoChange {
    pub(crate) token: usize,
    pub(crate) fd: RawFd,
    pub(crate) interest: Directions,
}
