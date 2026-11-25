use crate::{io::AsyncRead, selector::Direction};

use super::{NetError, socket::Socket};

/// Read half of a TCP/Unix stream.
#[must_use]
pub struct BorrowedReadHalf<'s, 'sel, const FDS: usize> {
    socket: &'s Socket<'sel, FDS>,
}

impl<'s, 'sel, const FDS: usize> BorrowedReadHalf<'s, 'sel, FDS> {
    pub(super) const fn new(socket: &'s Socket<'sel, FDS>) -> Self {
        Self { socket }
    }

    /// Waits until the stream is readable.
    ///
    /// The stream may still turn out to be not readable, so [`try_read`](Self::try_read)
    /// can return [`Errno::AGAIN`](rustix::io::Errno::AGAIN) after it.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn readable(&mut self) {
        self.socket.ready(Direction::Read).await;
    }

    /// Reads into a given buffer without waiting, returns the number of read bytes.
    ///
    /// Returns `0` if the stream is closed (or if the buffer is empty).
    ///
    /// # Errors
    ///
    /// Returns [`Errno::AGAIN`](rustix::io::Errno::AGAIN) if the stream is not readable,
    /// or an error if the syscall fails.
    pub fn try_read(&self, buf: &mut [u8]) -> rustix::io::Result<usize> {
        self.socket.try_recv(buf)
    }
}

impl<const FDS: usize> AsyncRead for BorrowedReadHalf<'_, '_, FDS> {
    type Error = NetError;

    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> impl Future<Output = Result<usize, NetError>> {
        self.socket.recv(buf)
    }
}
