use core::net::SocketAddr;

use rustix::{
    io::Errno,
    net::{SocketType, addr::SocketAddrArg},
};

use crate::context::Context;

use super::{NetError, TcpStream, socket::Socket};

/// A TCP listener.
#[must_use]
pub struct TcpListener<'sel, const FDS: usize> {
    socket: Socket<'sel, FDS>,
}

impl<'sel, const FDS: usize> TcpListener<'sel, FDS> {
    /// Binds on a given address with `SO_REUSEADDR` set, starts listening.
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
            SocketType::STREAM,
        )?;
        socket.set_reuseaddr()?;
        socket.bind(&addr)?;
        socket.listen()?;
        Ok(Self { socket })
    }

    /// Accepts incoming connection.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails or if all `FDS` fd slots are taken.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn accept(&mut self) -> Result<(TcpStream<'sel, FDS>, SocketAddr), NetError> {
        let (socket, addr) = self.socket.accept().await?;
        Ok((
            TcpStream::new(socket),
            addr.ok_or(Errno::AFNOSUPPORT)?.try_into()?,
        ))
    }

    /// Returns local address, useful when you bind on port 0.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}
