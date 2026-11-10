use rustix::fd::RawFd;

use crate::selector::{Directions, Token};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IoChange {
    token: Token,
    fd: RawFd,
    interest: Directions,
}

impl IoChange {
    pub(crate) fn new(token: Token, fd: RawFd, interest: Directions) -> Self {
        Self {
            token,
            fd,
            interest,
        }
    }

    pub(crate) fn token(self) -> Token {
        self.token
    }

    pub(crate) fn fd(self) -> RawFd {
        self.fd
    }

    pub(crate) fn interest(self) -> Directions {
        self.interest
    }
}
