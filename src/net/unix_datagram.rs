use rustix::net::{AddressFamily, SocketAddrUnix, SocketType};

use crate::{context::Context, net::socket::Socket, state::State};

pub struct UnixDatagram {
    socket: Socket,
}

impl UnixDatagram {
    pub fn bind(addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let socket = Socket::new(AddressFamily::UNIX, SocketType::DGRAM)?;
        socket.bind(addr)?;
        Ok(Self { socket })
    }

    pub fn unbound() -> rustix::io::Result<Self> {
        let socket = Socket::new(AddressFamily::UNIX, SocketType::DGRAM)?;
        Ok(Self { socket })
    }

    pub async fn send_to<
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
        buf: &[u8],
        addr: &SocketAddrUnix,
    ) -> rustix::io::Result<usize> {
        self.socket.send_to(ctx, buf, addr).await
    }

    pub async fn recv_from<
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
        buf: &mut [u8],
    ) -> rustix::io::Result<(usize, SocketAddrUnix)> {
        self.socket.recv_from(ctx, buf).await
    }

    pub fn connect(&self, addr: &SocketAddrUnix) -> rustix::io::Result<()> {
        self.socket.connect(addr)
    }

    pub async fn send<
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
        buf: &[u8],
    ) -> rustix::io::Result<usize> {
        self.socket.send(ctx, buf).await
    }

    pub async fn recv<
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
        buf: &mut [u8],
    ) -> rustix::io::Result<usize> {
        self.socket.recv(ctx, buf).await
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        self.socket.local_addr()
    }
}
