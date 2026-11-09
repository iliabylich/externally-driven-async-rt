use core::net::SocketAddr;

use rustix::net::Shutdown;

use crate::{
    context::Context,
    net::{ReadHalf, WriteHalf, socket::Socket},
    state::State,
};

pub struct TcpStream {
    socket: Socket,
}

impl TcpStream {
    pub(super) fn new(socket: Socket) -> Self {
        Self { socket }
    }

    pub async fn connect<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        ctx: &Context<
            '_,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        addr: SocketAddr,
    ) -> rustix::io::Result<Self> {
        let socket = Socket::connect_stream(ctx, &addr).await?;
        Ok(Self { socket })
    }

    pub async fn read<
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
        self.socket.read(ctx, buf).await
    }

    pub async fn write<
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
        self.socket.write(ctx, buf).await
    }

    pub fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        self.socket.shutdown(how)
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn peer_addr(&self) -> rustix::io::Result<SocketAddr> {
        self.socket.peer_addr()
    }

    pub fn into_split(self) -> rustix::io::Result<(ReadHalf, WriteHalf)> {
        let write = self.socket.try_clone()?;
        Ok((ReadHalf::new(self.socket), WriteHalf::new(write)))
    }
}
