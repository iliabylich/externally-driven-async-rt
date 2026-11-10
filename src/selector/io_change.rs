use rustix::fd::RawFd;

use crate::selector::{Directions, Token};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IoChange {
    pub(crate) token: Token,
    pub(crate) fd: RawFd,
    pub(crate) interest: Directions,
}
