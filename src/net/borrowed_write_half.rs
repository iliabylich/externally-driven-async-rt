use crate::{context::Context, io::AsyncWrite, net::socket::Socket, state::State};

/// Write half of a TCP/Unix stream.
#[must_use]
pub struct BorrowedWriteHalf<'s> {
    socket: &'s Socket,
}

impl<'s> BorrowedWriteHalf<'s> {
    pub(crate) const fn new(socket: &'s Socket) -> Self {
        Self { socket }
    }
}

impl AsyncWrite for BorrowedWriteHalf<'_> {
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
}

impl core::fmt::Debug for BorrowedWriteHalf<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("BorrowedWriteHalf")
    }
}
