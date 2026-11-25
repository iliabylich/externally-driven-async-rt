use core::{net::SocketAddr, time::Duration};

use rustix::{io::Errno, net::Shutdown};

use crate::{
    context::Context,
    io::{AsyncRead, AsyncWrite},
};

use super::{BorrowedReadHalf, BorrowedWriteHalf, NetError, socket::Socket};

/// A TCP stream.
#[must_use]
pub struct TcpStream<'sel, const FDS: usize> {
    socket: Socket<'sel, FDS>,
}

impl<'sel, const FDS: usize> TcpStream<'sel, FDS> {
    pub(super) const fn new(socket: Socket<'sel, FDS>) -> Self {
        Self { socket }
    }

    /// Connects to a given address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails or if all `FDS` fd slots are taken.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn connect<
        E: 'static,
        const EVENTS: usize,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
    >(
        ctx: &Context<'sel, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        addr: SocketAddr,
    ) -> Result<Self, NetError> {
        let socket = Socket::connect_stream(&ctx.storage.io_waiters, ctx.selector, &addr).await?;
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
    pub const fn split(
        &mut self,
    ) -> (
        BorrowedReadHalf<'_, 'sel, FDS>,
        BorrowedWriteHalf<'_, 'sel, FDS>,
    ) {
        (
            BorrowedReadHalf::new(&self.socket),
            BorrowedWriteHalf::new(&self.socket),
        )
    }
}

impl<const FDS: usize> AsyncRead for TcpStream<'_, FDS> {
    type Error = NetError;

    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> impl Future<Output = Result<usize, NetError>> {
        self.socket.recv(buf)
    }
}

impl<const FDS: usize> AsyncWrite for TcpStream<'_, FDS> {
    type Error = NetError;

    fn write<'a>(&'a mut self, buf: &'a [u8]) -> impl Future<Output = Result<usize, NetError>> {
        self.socket.send(buf)
    }

    fn flush(&mut self) -> impl Future<Output = Result<(), NetError>> {
        core::future::ready(Ok(()))
    }
}
