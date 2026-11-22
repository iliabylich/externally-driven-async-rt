use rustix::io::Errno;

use crate::NotEnoughResources;

/// An error returned from asynchronous socket operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetError {
    /// A syscall failed.
    Os(Errno),
    /// Statically allocated resources are not enough.
    NotEnoughResources(NotEnoughResources),
}

impl core::fmt::Display for NetError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Os(errno) => write!(f, "syscall failed: {errno}"),
            Self::NotEnoughResources(err) => write!(f, "{err}"),
        }
    }
}

impl core::error::Error for NetError {}

impl From<Errno> for NetError {
    fn from(errno: Errno) -> Self {
        Self::Os(errno)
    }
}

impl From<NotEnoughResources> for NetError {
    fn from(err: NotEnoughResources) -> Self {
        Self::NotEnoughResources(err)
    }
}
