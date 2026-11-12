use rustix::net::{AddressFamily, SocketAddrUnix, SocketType};

use crate::{context::Context, net::socket::Socket, state::State};

/// A Unix datagram socket.
#[must_use]
pub struct UnixDatagram {
    socket: Socket,
}

impl UnixDatagram {
    /// Binds on a given address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn bind(addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let socket = Socket::new(AddressFamily::UNIX, SocketType::DGRAM)?;
        socket.bind(addr)?;
        Ok(Self { socket })
    }

    /// Creates a socket that is not bound to any address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn unbound() -> rustix::io::Result<Self> {
        let socket = Socket::new(AddressFamily::UNIX, SocketType::DGRAM)?;
        Ok(Self { socket })
    }

    /// Sends a given buffer to a given address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub fn send_to<
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
        addr: &'a SocketAddrUnix,
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.socket.send_to(ctx, buf, addr)
    }

    /// Receives a datagram from any sender into a given buffer,
    /// returns its size and the sender address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
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
    ) -> impl Future<Output = rustix::io::Result<(usize, SocketAddrUnix)>> {
        let recv = self.socket.recv_from(ctx, buf);
        async move {
            let (len, addr) = recv.await?;
            let addr = match addr {
                Some(addr) => addr.try_into()?,
                None => SocketAddrUnix::new("")?,
            };
            Ok((len, addr))
        }
    }

    /// Connects to a given address, which makes it the peer of [`send`](Self::send) and [`recv`](Self::recv).
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn connect(&self, addr: &SocketAddrUnix) -> rustix::io::Result<()> {
        self.socket.connect(addr)
    }

    /// Sends a given buffer to the connected peer, see [`connect`](Self::connect).
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
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
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
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
    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        self.socket.local_addr()
    }
}

impl core::fmt::Debug for UnixDatagram {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("UnixDatagram")
    }
}
