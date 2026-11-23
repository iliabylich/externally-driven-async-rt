use crate::{
    NotEnoughResources,
    context::Context,
    io::AsyncWrite,
    net::{NetError, socket::Socket},
    selector::Direction,
};

/// Write half of a TCP/Unix stream.
#[must_use]
pub struct BorrowedWriteHalf<'s> {
    socket: &'s Socket,
}

impl<'s> BorrowedWriteHalf<'s> {
    pub(super) const fn new(socket: &'s Socket) -> Self {
        Self { socket }
    }

    /// Waits until the stream is writable.
    ///
    /// The stream may still turn out to be not writable, so [`try_write`](Self::try_write)
    /// can return [`Errno::AGAIN`](rustix::io::Errno::AGAIN) after it.
    ///
    /// # Errors
    ///
    /// Returns an error if all `FDS` fd slots are taken.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn writable<
        E: 'static,
        const EVENTS: usize,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &mut self,
        ctx: &Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    ) -> Result<(), NotEnoughResources> {
        self.socket
            .ready(&ctx.storage.io_waiters, ctx.selector, Direction::Write)
            .await
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

impl AsyncWrite for BorrowedWriteHalf<'_> {
    type Error = NetError;

    fn write<
        'a,
        E: 'static,
        const EVENTS: usize,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        ctx: &'a Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, NetError>> {
        self.socket.send(&ctx.storage.io_waiters, ctx.selector, buf)
    }

    fn flush<
        'a,
        E: 'static,
        const EVENTS: usize,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        _ctx: &'a Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    ) -> impl Future<Output = Result<(), NetError>> {
        core::future::ready(Ok(()))
    }
}
