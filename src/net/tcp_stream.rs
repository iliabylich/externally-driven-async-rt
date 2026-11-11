use core::net::SocketAddr;

use rustix::{io::Errno, net::Shutdown};

use crate::{
    context::Context,
    net::{BorrowedReadHalf, BorrowedWriteHalf, socket::Socket},
    state::State,
};

/// A TCP stream.
pub struct TcpStream {
    socket: Socket,
}

impl TcpStream {
    pub(crate) fn new(socket: Socket) -> Self {
        Self { socket }
    }

    /// Connects to a given address.
    pub async fn connect<
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        ctx: &Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        addr: SocketAddr,
    ) -> rustix::io::Result<Self> {
        let socket = Socket::connect_stream(ctx, &addr).await?;
        Ok(Self { socket })
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

    /// Writes from a given buffer into the underlying socket.
    pub fn write<
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
        buf: &'a [u8],
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.socket.write(ctx, buf)
    }

    /// Shuts down the connection (which also impacts potentially existing read/write halves).
    ///
    /// The underlying file descriptor stays open until this value is dropped.
    pub fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        self.socket.shutdown(how)
    }

    /// Returns local address.
    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Returns peer address.
    pub fn peer_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket
            .peer_addr()?
            .ok_or(Errno::AFNOSUPPORT)?
            .try_into()
    }

    /// Splits the [`TcpStream`] into two halves: reader and writer.
    pub fn split(&mut self) -> (BorrowedReadHalf<'_>, BorrowedWriteHalf<'_>) {
        (
            BorrowedReadHalf::new(&self.socket),
            BorrowedWriteHalf::new(&self.socket),
        )
    }
}

impl core::fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("TcpStream")
    }
}
