use rustix::{fd::OwnedFd, net::Shutdown};

use crate::{
    context::Context,
    net::{read, write},
    state::State,
};

pub struct ReadHalf {
    fd: OwnedFd,
}

impl ReadHalf {
    pub(crate) fn new(fd: OwnedFd) -> Self {
        Self { fd }
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
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        buf: &mut [u8],
    ) -> rustix::io::Result<usize> {
        read(ctx, &self.fd, buf).await
    }
}

pub struct WriteHalf {
    fd: OwnedFd,
}

impl WriteHalf {
    pub(crate) fn new(fd: OwnedFd) -> Self {
        Self { fd }
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
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        buf: &[u8],
    ) -> rustix::io::Result<usize> {
        write(ctx, &self.fd, buf).await
    }

    pub fn shutdown(&self) -> rustix::io::Result<()> {
        rustix::net::shutdown(&self.fd, Shutdown::Write)
    }
}

impl Drop for WriteHalf {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
