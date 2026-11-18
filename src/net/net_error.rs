use rustix::io::Errno;

use crate::io::FdsFullError;

/// An error returned from asynchronous socket operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetError {
    /// A syscall failed.
    Os(Errno),
    /// All `FDS` fd slots are taken.
    FdsFull,
}

impl core::fmt::Display for NetError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Os(errno) => write!(f, "syscall failed: {errno}"),
            Self::FdsFull => f.write_str("all fd slots are taken"),
        }
    }
}

impl core::error::Error for NetError {}

impl From<Errno> for NetError {
    fn from(errno: Errno) -> Self {
        Self::Os(errno)
    }
}

impl From<FdsFullError> for NetError {
    fn from(_: FdsFullError) -> Self {
        Self::FdsFull
    }
}
