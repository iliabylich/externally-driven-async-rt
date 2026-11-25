use crate::{io::AsyncWrite, selector::Direction};

use super::{NetError, socket::Socket};

/// Write half of a TCP/Unix stream.
#[must_use]
pub struct BorrowedWriteHalf<'s, 'sel, const FDS: usize> {
    socket: &'s Socket<'sel, FDS>,
}

impl<'s, 'sel, const FDS: usize> BorrowedWriteHalf<'s, 'sel, FDS> {
    pub(super) const fn new(socket: &'s Socket<'sel, FDS>) -> Self {
        Self { socket }
    }

    /// Waits until the stream is writable.
    ///
    /// The stream may still turn out to be not writable, so [`try_write`](Self::try_write)
    /// can return [`Errno::AGAIN`](rustix::io::Errno::AGAIN) after it.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn writable(&mut self) {
        self.socket.ready(Direction::Write).await;
    }

    /// Writes from a given buffer without waiting, returns the number of written bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Errno::AGAIN`](rustix::io::Errno::AGAIN) if the stream is not writable,
    /// or an error if the syscall fails.
    pub fn try_write(&self, buf: &[u8]) -> rustix::io::Result<usize> {
        self.socket.try_send(buf)
    }
}

impl<const FDS: usize> AsyncWrite for BorrowedWriteHalf<'_, '_, FDS> {
    type Error = NetError;

    fn write<'a>(&'a mut self, buf: &'a [u8]) -> impl Future<Output = Result<usize, NetError>> {
        self.socket.send(buf)
    }

    fn flush(&mut self) -> impl Future<Output = Result<(), NetError>> {
        core::future::ready(Ok(()))
    }
}
