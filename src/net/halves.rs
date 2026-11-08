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

    pub async fn read<S: State>(
        &mut self,
        ctx: &Context<S>,
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

    pub async fn write<S: State>(
        &mut self,
        ctx: &Context<S>,
        buf: &[u8],
    ) -> rustix::io::Result<usize> {
        write(ctx, &self.fd, buf).await
    }

    pub fn shutdown(&self) -> rustix::io::Result<()> {
        rustix::net::shutdown(&self.fd, Shutdown::Write)
    }
}

// Each half owns its own `dup` of the socket, so closing one fd doesn't end the connection.
// Shutting down the write direction makes dropping the write half visible to the peer (EOF).
impl Drop for WriteHalf {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
