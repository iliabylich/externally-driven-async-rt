/// An error that can be returned from an [`EventLoop`](crate::EventLoop).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// An epoll or kqueue call failed.
    Io(rustix::io::Errno),
    /// [`EventLoop::drain_events_without_blocking`](crate::EventLoop::drain_events_without_blocking)
    /// is called on the [`EventLoop`](crate::EventLoop) that has already finished
    RootFinished,
    /// [`EventLoop::new`](crate::EventLoop::new) is called with a
    /// [`LoopStorage`](crate::LoopStorage) that has already been used by another
    /// [`EventLoop`](crate::EventLoop). A storage backs at most one event loop for the whole
    /// program, even after that loop has finished.
    StorageAlreadyUsed,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "event loop IO error: {errno}"),
            Self::RootFinished => f.write_str("the root future has finished"),
            Self::StorageAlreadyUsed => {
                f.write_str("the storage has already been used by another event loop")
            }
        }
    }
}

impl core::error::Error for Error {}

impl From<rustix::io::Errno> for Error {
    fn from(errno: rustix::io::Errno) -> Self {
        Self::Io(errno)
    }
}
