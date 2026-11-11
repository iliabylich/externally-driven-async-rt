use rustix::net::{AddressFamily, SocketAddrUnix, SocketType};

use crate::{
    context::Context,
    net::{UnixStream, socket::Socket},
    state::State,
};

/// A Unix stream listener.
pub struct UnixListener {
    socket: Socket,
}

impl UnixListener {
    /// Binds on a given address, starts listening.
    pub fn bind(addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let socket = Socket::new(AddressFamily::UNIX, SocketType::STREAM)?;
        socket.bind(addr)?;
        socket.listen()?;
        Ok(Self { socket })
    }

    /// Accepts incoming connection.
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
    ) -> rustix::io::Result<(UnixStream, SocketAddrUnix)> {
        let (socket, addr) = self.socket.accept(ctx).await?;
        let addr = match addr {
            Some(addr) => addr.try_into()?,
            None => SocketAddrUnix::new("")?,
        };
        Ok((UnixStream::new(socket), addr))
    }

    /// Returns local address.
    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        self.socket.local_addr()
    }
}

impl core::fmt::Debug for UnixListener {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("UnixListener")
    }
}
