use core::net::SocketAddr;

use rustix::net::SocketType;

use crate::{
    context::Context,
    net::{address::Address, socket::Socket},
    state::State,
};

/// A UDP socket.
pub struct UdpSocket {
    socket: Socket,
}

impl UdpSocket {
    /// Binds on a given address.
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let socket = Socket::new(addr.family(), SocketType::DGRAM)?;
        socket.bind(&addr)?;
        Ok(Self { socket })
    }

    /// Sends a given buffer to a given address.
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
    pub fn recv_from<
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
    ) -> impl Future<Output = rustix::io::Result<(usize, SocketAddr)>> {
        self.socket.recv_from(ctx, buf)
    }

    /// Connects to a given address, which makes it the peer of [`send`](Self::send) and [`recv`](Self::recv).
    pub fn connect(&self, addr: SocketAddr) -> rustix::io::Result<()> {
        self.socket.connect(&addr)
    }

    /// Sends a given buffer to the connected peer, see [`connect`](Self::connect).
    pub fn send<
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
        self.socket.send(ctx, buf)
    }

    /// Receives a datagram from the connected peer into a given buffer, see [`connect`](Self::connect).
    pub fn recv<
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
        self.socket.recv(ctx, buf)
    }

    /// Returns local address.
    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl core::fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("UdpSocket")
    }
}
