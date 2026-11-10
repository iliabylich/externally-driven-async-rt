use crate::{context::Context, net::socket::Socket, state::State};

/// Read half of a TCP/Unix stream.
pub struct BorrowedReadHalf<'s> {
    socket: &'s Socket,
}

impl<'s> BorrowedReadHalf<'s> {
    pub(crate) fn new(socket: &'s Socket) -> Self {
        Self { socket }
    }

    /// Reads from the underlying socket into a given buffer.
    pub fn read<
        'a,
        'ctx,
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        ctx: &'a Context<'ctx, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
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
