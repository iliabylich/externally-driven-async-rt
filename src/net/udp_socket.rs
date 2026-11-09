use core::net::SocketAddr;

use rustix::net::SocketType;

use crate::{
    context::Context,
    net::{address::Address, socket::Socket},
    state::State,
};

pub struct UdpSocket {
    socket: Socket,
}

impl UdpSocket {
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let socket = Socket::new(addr.family(), SocketType::DGRAM)?;
        socket.bind(&addr)?;
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
        addr: SocketAddr,
    ) -> rustix::io::Result<usize> {
        self.socket.send_to(ctx, buf, &addr).await
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
    ) -> impl Future<Output = rustix::io::Result<(usize, SocketAddr)>> {
        self.socket.recv_from(ctx, buf)
    }

    pub fn connect(&self, addr: SocketAddr) -> rustix::io::Result<()> {
        self.socket.connect(&addr)
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

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}
