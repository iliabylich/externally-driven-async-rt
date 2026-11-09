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

    pub fn send_to<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a mut self,
        ctx: &'a Context<
            'ctx,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        buf: &'a [u8],
        addr: &'a SocketAddrUnix,
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.socket.send_to(ctx, buf, addr)
    }

    pub fn recv_from<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a mut self,
        ctx: &'a Context<
            'ctx,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        buf: &'a mut [u8],
    ) -> impl Future<Output = rustix::io::Result<(usize, SocketAddrUnix)>> {
        self.socket.recv_from(ctx, buf)
    }

    pub fn connect(&self, addr: &SocketAddrUnix) -> rustix::io::Result<()> {
        self.socket.connect(addr)
    }

    pub fn send<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a mut self,
        ctx: &'a Context<
            'ctx,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        buf: &'a [u8],
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.socket.send(ctx, buf)
    }

    pub fn recv<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a mut self,
        ctx: &'a Context<
            'ctx,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        buf: &'a mut [u8],
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.socket.recv(ctx, buf)
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        self.socket.local_addr()
    }
}
