use core::net::SocketAddr;

use rustix::{
    io::Errno,
    net::{SocketType, addr::SocketAddrArg},
};

use crate::{context::Context, net::socket::Socket, state::State};

/// A UDP socket.
#[must_use]
pub struct UdpSocket {
    socket: Socket,
}

impl UdpSocket {
    /// Binds on a given address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let socket = Socket::new(addr.as_any().address_family(), SocketType::DGRAM)?;
        socket.bind(&addr)?;
        Ok(Self { socket })
    }

    /// Sends a given buffer to a given address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub async fn send_to<
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &mut self,
        ctx: &Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> rustix::io::Result<usize> {
        self.socket.send_to(ctx, buf, &addr).await
    }

    /// Receives a datagram from any sender into a given buffer, returns its size and the sender address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn recv_from<
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
    ) -> impl Future<Output = rustix::io::Result<(usize, SocketAddr)>> {
        let recv = self.socket.recv_from(ctx, buf);
        async move {
            let (len, addr) = recv.await?;
            Ok((len, addr.ok_or(Errno::AFNOSUPPORT)?.try_into()?))
        }
    }

    /// Connects to a given address, which makes it the peer of [`send`](Self::send) and [`recv`](Self::recv).
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn connect(&self, addr: SocketAddr) -> rustix::io::Result<()> {
        self.socket.connect(&addr)
    }

    /// Sends a given buffer to the connected peer, see [`connect`](Self::connect).
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn send<
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
        self.socket.send(ctx, buf)
    }

    /// Receives a datagram from the connected peer into a given buffer, see [`connect`](Self::connect).
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn recv<
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
        self.socket.recv(ctx, buf)
    }

    /// Returns local address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl core::fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("UdpSocket")
    }
}
