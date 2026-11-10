/// An error that can be returned from an [`EventLoop`](crate::EventLoop).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// An epoll or kqueue call failed.
    Io(rustix::io::Errno),
    /// [`EventLoop::drain_events_without_blocking`](crate::EventLoop::drain_events_without_blocking)
    /// is called on the [`EventLoop`](crate::EventLoop) that has already finished
    RootFinished,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "event loop IO error: {errno}"),
            Self::RootFinished => f.write_str("the root future has finished"),
        }
    }
}

impl core::error::Error for Error {}
