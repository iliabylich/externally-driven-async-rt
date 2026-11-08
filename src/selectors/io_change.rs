use rustix::fd::RawFd;

use crate::selectors::Token;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IoAction {
    Arm,
    Disarm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IoChange {
    pub(crate) token: Token,
    pub(crate) fd: RawFd,
    pub(crate) direction: Direction,
    pub(crate) action: IoAction,
}
