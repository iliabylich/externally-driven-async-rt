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

#[cfg(test)]
mod tests {
    use super::Error;

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
            (Error::RootFinished, "the root future has finished"),
            (Error::Io(rustix::io::Errno::BADF), "event loop IO error: "),
            (
                Error::StorageAlreadyUsed,
                "the storage has already been used by another event loop",
            ),
        ] {
            let mut prefix = StartsWith(expected);
            core::fmt::write(&mut prefix, format_args!("{error}")).unwrap();
            assert_eq!(prefix.0, "", "{error:?}");
        }
    }
}
