use core::net::SocketAddr;

use rustix::net::SocketType;

use crate::{
    context::Context,
    net::{TcpStream, address::Address, socket::Socket},
    state::State,
};

/// A TCP listener.
pub struct TcpListener {
    socket: Socket,
}

impl TcpListener {
    /// Binds on a given address with `SO_REUSEADDR` set, starts listening.
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let socket = Socket::new(addr.family(), SocketType::STREAM)?;
        socket.set_reuseaddr()?;
        socket.bind(&addr)?;
        socket.listen()?;
        Ok(Self { socket })
    }

    /// Accepts incoming connection.
    pub async fn accept<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &mut self,
        ctx: &Context<
            '_,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
    ) -> rustix::io::Result<(TcpStream, SocketAddr)> {
        let (socket, addr) = self.socket.accept(ctx).await?;
        Ok((TcpStream::new(socket), addr))
    }

    /// Returns local address, useful when you bind on port 0.
    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl core::fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("TcpListener")
    }
}
