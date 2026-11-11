use rustix::fd::RawFd;

use crate::selector::Directions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IoChange {
    token: usize,
    fd: RawFd,
    interest: Directions,
}

impl IoChange {
    pub(crate) fn new(token: usize, fd: RawFd, interest: Directions) -> Self {
        Self {
            token,
            fd,
            interest,
        }
    }

    pub(crate) fn token(self) -> usize {
        self.token
    }

    pub(crate) fn fd(self) -> RawFd {
        self.fd
    }

    pub(crate) fn interest(self) -> Directions {
        self.interest
    }
}
