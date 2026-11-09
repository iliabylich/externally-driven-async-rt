use core::net::SocketAddr;

use rustix::net::SocketType;

use crate::{
    context::Context,
    net::{TcpStream, address::Address, socket::Socket},
    state::State,
};

pub struct TcpListener {
    socket: Socket,
}

impl TcpListener {
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let socket = Socket::new(addr.family(), SocketType::STREAM)?;
        socket.set_reuseaddr()?;
        socket.bind(&addr)?;
        socket.listen()?;
        Ok(Self { socket })
    }

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

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}
