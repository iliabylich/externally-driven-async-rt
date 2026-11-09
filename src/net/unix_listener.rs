use rustix::net::{AddressFamily, SocketAddrUnix, SocketType};

use crate::{
    context::Context,
    net::{UnixStream, socket::Socket},
    state::State,
};

pub struct UnixListener {
    socket: Socket,
}

impl UnixListener {
    pub fn bind(addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let socket = Socket::new(AddressFamily::UNIX, SocketType::STREAM)?;
        socket.bind(addr)?;
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
    ) -> rustix::io::Result<(UnixStream, SocketAddrUnix)> {
        let (socket, addr) = self.socket.accept(ctx).await?;
        Ok((UnixStream::new(socket), addr))
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        self.socket.local_addr()
    }
}
