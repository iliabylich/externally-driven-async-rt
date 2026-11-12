use crate::{
    context::Context, io::AsyncRead, net::socket::Socket, selector::Direction, state::State,
};

/// Read half of a TCP/Unix stream.
#[must_use]
pub struct BorrowedReadHalf<'s> {
    socket: &'s Socket,
}

impl<'s> BorrowedReadHalf<'s> {
    pub(crate) const fn new(socket: &'s Socket) -> Self {
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
    pub async fn readable<
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &self,
        ctx: &Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    ) {
        self.socket.ready(ctx, Direction::Read).await;
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
        self.socket.try_read(buf)
    }
}

impl AsyncRead for BorrowedReadHalf<'_> {
    type Error = rustix::io::Errno;

    fn read<
        'a,
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        ctx: &'a Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        buf: &'a mut [u8],
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.socket.read(ctx, buf)
    }
}

impl core::fmt::Debug for BorrowedReadHalf<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("BorrowedReadHalf")
    }
}
