use core::{net::SocketAddr, time::Duration};

use rustix::{io::Errno, net::Shutdown};

use crate::{
    context::Context,
    io::{AsyncRead, AsyncWrite},
    net::{BorrowedReadHalf, BorrowedWriteHalf, socket::Socket},
    state::State,
};

/// A TCP stream.
#[must_use]
pub struct TcpStream {
    socket: Socket,
}

impl TcpStream {
    pub(crate) const fn new(socket: Socket) -> Self {
        Self { socket }
    }

    /// Connects to a given address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
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

    /// Shuts down the connection (which also impacts potentially existing read/write halves).
    ///
    /// The underlying file descriptor stays open until this value is dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        self.socket.shutdown(how)
    }

    /// Enables TCP keepalive.
    ///
    /// Once the connection is idle for `idle`, a probe is sent every `interval`, and the
    /// connection is considered dead after `count` unanswered probes. On Linux the same total
    /// (`idle + interval * count`) also limits how long sent data may stay unacknowledged
    /// (`TCP_USER_TIMEOUT`).
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails, or if the total doesn't fit into `TCP_USER_TIMEOUT`.
    pub fn set_keepalive(
        &self,
        idle: Duration,
        interval: Duration,
        count: u32,
    ) -> rustix::io::Result<()> {
        self.socket.set_keepalive(idle, interval, count)
    }

    /// Returns local address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Returns peer address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn peer_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket
            .peer_addr()?
            .ok_or(Errno::AFNOSUPPORT)?
            .try_into()
    }

    /// Splits the [`TcpStream`] into two halves: reader and writer.
    pub const fn split(&mut self) -> (BorrowedReadHalf<'_>, BorrowedWriteHalf<'_>) {
        (
            BorrowedReadHalf::new(&self.socket),
            BorrowedWriteHalf::new(&self.socket),
        )
    }
}

impl AsyncRead for TcpStream {
    type Error = Errno;

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

impl AsyncWrite for TcpStream {
    type Error = Errno;

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

impl core::fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("TcpStream")
    }
}
