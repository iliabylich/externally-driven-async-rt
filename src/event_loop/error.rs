/// An error that can be returned from an [`EventLoop`](crate::EventLoop).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventLoopError {
    /// An epoll or kqueue call failed.
    Io(rustix::io::Errno),
    /// The root future has finished, either in [`EventLoop::new`](crate::EventLoop::new) or in
    /// [`EventLoop::drain_events_without_blocking`](crate::EventLoop::drain_events_without_blocking).
    /// Every later drain returns it too.
    RootFinished,
    /// [`EventLoop::new`](crate::EventLoop::new) is called with a
    /// [`LoopStorage`](crate::LoopStorage) that has already been used by another
    /// [`EventLoop`](crate::EventLoop). A storage backs at most one event loop for the whole
    /// program, even after that loop has finished.
    StorageAlreadyUsed,
    /// [`EventLoop::drain_events_without_blocking`](crate::EventLoop::drain_events_without_blocking)
    /// panicked before, the [`EventLoop`](crate::EventLoop) may be in an inconsistent state, so it
    /// refuses to run. Its tasks are dropped once it's dropped.
    Poisoned,
}

impl core::fmt::Display for EventLoopError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "event loop IO error: {errno}"),
            Self::RootFinished => f.write_str("the root future has finished"),
            Self::StorageAlreadyUsed => {
                f.write_str("the storage has already been used by another event loop")
            }
            Self::Poisoned => f.write_str("the event loop panicked before and is poisoned"),
        }
    }
}

impl core::error::Error for EventLoopError {}

impl From<rustix::io::Errno> for EventLoopError {
    fn from(errno: rustix::io::Errno) -> Self {
        Self::Io(errno)
    }
}

#[cfg(test)]
mod tests {
    use super::EventLoopError;

    #[test]
    fn errors_display_their_cause() {
        struct StartsWith<'a>(&'a str);

        impl core::fmt::Write for StartsWith<'_> {
            fn write_str(&mut self, s: &str) -> core::fmt::Result {
                match self.0.strip_prefix(s) {
                    Some(rest) => self.0 = rest,
                    None if self.0.is_empty() => {}
                    None => return Err(core::fmt::Error),
                }
                Ok(())
            }
        }

        for (error, expected) in [
            (EventLoopError::RootFinished, "the root future has finished"),
            (
                EventLoopError::Io(rustix::io::Errno::BADF),
                "event loop IO error: ",
            ),
            (
                EventLoopError::StorageAlreadyUsed,
                "the storage has already been used by another event loop",
            ),
            (
                EventLoopError::Poisoned,
                "the event loop panicked before and is poisoned",
            ),
        ] {
            let mut prefix = StartsWith(expected);
            core::fmt::write(&mut prefix, format_args!("{error}")).unwrap();
            assert_eq!(prefix.0, "", "{error:?}");
        }
    }
}
