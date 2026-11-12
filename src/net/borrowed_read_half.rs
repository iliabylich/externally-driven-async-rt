use crate::{context::Context, io::AsyncRead, net::socket::Socket, state::State};

/// Read half of a TCP/Unix stream.
#[must_use]
pub struct BorrowedReadHalf<'s> {
    socket: &'s Socket,
}

impl<'s> BorrowedReadHalf<'s> {
    pub(crate) const fn new(socket: &'s Socket) -> Self {
        Self { socket }
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
