use crate::{
    context::Context, io::AsyncWrite, net::socket::Socket, selector::Direction, state::State,
};

/// Write half of a TCP/Unix stream.
#[must_use]
pub struct BorrowedWriteHalf<'s> {
    socket: &'s Socket,
}

impl<'s> BorrowedWriteHalf<'s> {
    pub(crate) const fn new(socket: &'s Socket) -> Self {
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
    pub async fn writable<
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
        self.socket.ready(ctx, Direction::Write).await;
    }

    /// Writes from a given buffer without waiting, returns the number of written bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Errno::AGAIN`](rustix::io::Errno::AGAIN) if the stream is not writable,
    /// or an error if the syscall fails.
    pub fn try_write(&self, buf: &[u8]) -> rustix::io::Result<usize> {
        self.socket.try_write(buf)
    }
}

impl AsyncWrite for BorrowedWriteHalf<'_> {
    type Error = rustix::io::Errno;

    fn write<
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
        buf: &'a [u8],
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.socket.write(ctx, buf)
    }

    fn flush<
        'a,
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        _ctx: &'a Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    ) -> impl Future<Output = rustix::io::Result<()>> {
        core::future::ready(Ok(()))
    }
}

impl core::fmt::Debug for BorrowedWriteHalf<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("BorrowedWriteHalf")
    }
}
