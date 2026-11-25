use core::net::SocketAddr;

use rustix::{
    io::Errno,
    net::{SocketType, addr::SocketAddrArg},
};

use crate::context::Context;

use super::{NetError, socket::Socket};

/// A UDP socket.
#[must_use]
pub struct UdpSocket<'sel, const FDS: usize> {
    socket: Socket<'sel, FDS>,
}

impl<'sel, const FDS: usize> UdpSocket<'sel, FDS> {
    /// Binds on a given address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails or if all `FDS` fd slots are taken.
    pub fn bind<
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
        let socket = Socket::new(
            &ctx.storage.io_waiters,
            ctx.selector,
            addr.as_any().address_family(),
            SocketType::DGRAM,
        )?;
        socket.bind(&addr)?;
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
    pub async fn send_to(&mut self, buf: &[u8], addr: SocketAddr) -> Result<usize, NetError> {
        self.socket.send_to(buf, &addr).await
    }

    /// Receives a datagram from any sender into a given buffer, returns its size and the sender address.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub fn recv_from<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<(usize, SocketAddr), NetError>> {
        let recv = self.socket.recv_from(buf);
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
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub fn send<'a>(&'a mut self, buf: &'a [u8]) -> impl Future<Output = Result<usize, NetError>> {
        self.socket.send(buf)
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
    pub fn recv<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, NetError>> {
        self.socket.recv(buf)
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
