use rustix::net::{AddressFamily, SocketAddrUnix, SocketType};

use crate::context::Context;

use super::{NetError, UnixStream, socket::Socket};

/// A Unix stream listener.
#[must_use]
pub struct UnixListener<'sel, const FDS: usize> {
    socket: Socket<'sel, FDS>,
}

impl<'sel, const FDS: usize> UnixListener<'sel, FDS> {
    /// Binds on a given address, starts listening.
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
        addr: &SocketAddrUnix,
    ) -> Result<Self, NetError> {
        let socket = Socket::new(
            &ctx.storage.io_waiters,
            ctx.selector,
            AddressFamily::UNIX,
            SocketType::STREAM,
        )?;
        socket.bind(addr)?;
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
    pub async fn accept(&mut self) -> Result<(UnixStream<'sel, FDS>, SocketAddrUnix), NetError> {
        let (socket, addr) = self.socket.accept().await?;
        let addr = match addr {
            Some(addr) => addr.try_into()?,
            None => SocketAddrUnix::new("")?,
        };
        Ok((UnixStream::new(socket), addr))
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
