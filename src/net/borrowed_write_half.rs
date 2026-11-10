use crate::{context::Context, net::socket::Socket, state::State};

/// Write half of a TCP/Unix stream.
pub struct BorrowedWriteHalf<'s> {
    socket: &'s Socket,
}

impl<'s> BorrowedWriteHalf<'s> {
    pub(crate) fn new(socket: &'s Socket) -> Self {
        Self { socket }
    }

    /// Writes from a given buffer into the underlying socket.
    pub fn write<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a mut self,
        ctx: &'a Context<
            'ctx,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
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
