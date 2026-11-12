use core::net::SocketAddr;

use rustix::{
    io::Errno,
    net::{SocketType, addr::SocketAddrArg},
};

use crate::{
    context::Context,
    net::{TcpStream, socket::Socket},
    state::State,
};

/// A TCP listener.
#[must_use]
pub struct TcpListener {
    socket: Socket,
}

impl TcpListener {
    /// Binds on a given address with `SO_REUSEADDR` set, starts listening.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let socket = Socket::new(addr.as_any().address_family(), SocketType::STREAM)?;
        socket.set_reuseaddr()?;
        socket.bind(&addr)?;
        socket.listen()?;
        Ok(Self { socket })
    }

    /// Accepts incoming connection.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn accept<
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &mut self,
        ctx: &Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    ) -> rustix::io::Result<(TcpStream, SocketAddr)> {
        let (socket, addr) = self.socket.accept(ctx).await?;
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

impl core::fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("TcpListener")
    }
}
