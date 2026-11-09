#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Io(rustix::io::Errno),
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
