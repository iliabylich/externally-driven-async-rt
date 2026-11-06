use rustix::fd::OwnedFd;
use rustix::net::Shutdown;

use crate::context::Context;

pub struct ReadHalf {
    fd: OwnedFd,
}

impl ReadHalf {
    pub(super) fn new(fd: OwnedFd) -> Self {
        Self { fd }
    }

    pub async fn read<E>(&mut self, ctx: &Context<E>, buf: &mut [u8]) -> rustix::io::Result<usize> {
        super::read(ctx, &self.fd, buf).await
    }
}

pub struct WriteHalf {
    fd: OwnedFd,
}

impl WriteHalf {
    pub(super) fn new(fd: OwnedFd) -> Self {
        Self { fd }
    }

    pub async fn write<E>(&mut self, ctx: &Context<E>, buf: &[u8]) -> rustix::io::Result<usize> {
        super::write(ctx, &self.fd, buf).await
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
