use rustix::{
    fd::{AsFd, OwnedFd},
    io::{Errno, FdFlags, fcntl_dupfd_cloexec, fcntl_setfd, ioctl_fionbio},
    net::{
        AddressFamily, RecvFlags, SendFlags, Shutdown, SocketType,
        sockopt::{set_socket_nosigpipe, set_socket_reuseaddr, socket_error},
    },
};

use crate::{
    context::Context, io::IoRegistration, net::address::Address, selector::Direction, state::State,
};

const LISTEN_BACKLOG: i32 = 1024;

pub(super) struct Socket(OwnedFd);

impl Socket {
    pub(super) fn new(family: AddressFamily, ty: SocketType) -> rustix::io::Result<Self> {
        let socket = Self(rustix::net::socket(family, ty, None)?);
        socket.configure(ty)?;
        Ok(socket)
    }

    pub(super) fn bind<A: Address>(&self, addr: &A) -> rustix::io::Result<()> {
        rustix::net::bind(&self.0, addr)
    }

    pub(super) fn listen(&self) -> rustix::io::Result<()> {
        rustix::net::listen(&self.0, LISTEN_BACKLOG)
    }

    pub(super) fn set_reuseaddr(&self) -> rustix::io::Result<()> {
        set_socket_reuseaddr(&self.0, true)
    }

    pub(super) fn connect<A: Address>(&self, addr: &A) -> rustix::io::Result<()> {
        rustix::net::connect(&self.0, addr)
    }

    pub(super) async fn connect_stream<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
        A: Address,
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
        addr: &A,
    ) -> rustix::io::Result<Self> {
        let socket = Self::new(addr.family(), SocketType::STREAM)?;
        match rustix::net::connect(&socket.0, addr) {
            Ok(()) => return Ok(socket),
            Err(Errno::INPROGRESS | Errno::INTR) => {}
            Err(errno) => return Err(errno),
        }
        IoRegistration::new(ctx.storage(), socket.0.as_fd(), Direction::Write)
            .wait()
            .await;
        socket_error(&socket.0)??;
        Ok(socket)
    }

    pub(super) async fn accept<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
        A: Address,
    >(
        &self,
        ctx: &Context<
            '_,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
    ) -> rustix::io::Result<(Self, A)> {
        let (fd, addr) = self
            .retry(ctx, Direction::Read, |fd| rustix::net::acceptfrom(fd))
            .await?;
        let socket = Self(fd);
        socket.configure(SocketType::STREAM)?;
        Ok((socket, A::from_any(addr)?))
    }

    pub(super) async fn read<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &self,
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
        self.retry(ctx, Direction::Read, |fd| rustix::io::read(fd, &mut *buf))
            .await
    }

    pub(super) async fn write<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &self,
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
        self.retry(ctx, Direction::Write, |fd| rustix::io::write(fd, buf))
            .await
    }

    pub(super) async fn send<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &self,
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
        self.retry(ctx, Direction::Write, |fd| {
            rustix::net::send(fd, buf, SendFlags::empty())
        })
        .await
    }

    pub(super) async fn recv<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &self,
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
        let (len, _) = self
            .retry(ctx, Direction::Read, |fd| {
                rustix::net::recv(fd, &mut *buf, RecvFlags::empty())
            })
            .await?;
        Ok(len)
    }

    pub(super) async fn send_to<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
        A: Address,
    >(
        &self,
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
        addr: &A,
    ) -> rustix::io::Result<usize> {
        self.retry(ctx, Direction::Write, |fd| {
            rustix::net::sendto(fd, buf, SendFlags::empty(), addr)
        })
        .await
    }

    pub(super) async fn recv_from<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
        A: Address,
    >(
        &self,
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
    ) -> rustix::io::Result<(usize, A)> {
        let (len, _, addr) = self
            .retry(ctx, Direction::Read, |fd| {
                rustix::net::recvfrom(fd, &mut *buf, RecvFlags::empty())
            })
            .await?;
        Ok((len, A::from_any(addr)?))
    }

    pub(super) fn local_addr<A: Address>(&self) -> rustix::io::Result<A> {
        A::from_any(Some(rustix::net::getsockname(&self.0)?))
    }

    pub(super) fn peer_addr<A: Address>(&self) -> rustix::io::Result<A> {
        A::from_any(rustix::net::getpeername(&self.0)?)
    }

    pub(super) fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        rustix::net::shutdown(&self.0, how)
    }

    pub(super) fn try_clone(&self) -> rustix::io::Result<Self> {
        Ok(Self(fcntl_dupfd_cloexec(&self.0, 0)?))
    }

    fn configure(&self, ty: SocketType) -> rustix::io::Result<()> {
        fcntl_setfd(&self.0, FdFlags::CLOEXEC)?;
        ioctl_fionbio(&self.0, true)?;
        if ty == SocketType::STREAM {
            set_socket_nosigpipe(&self.0, true)?;
        }
        Ok(())
    }

    async fn retry<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
        T,
    >(
        &self,
        ctx: &Context<
            '_,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        direction: Direction,
        mut op: impl FnMut(&OwnedFd) -> rustix::io::Result<T>,
    ) -> rustix::io::Result<T> {
        let mut registration = IoRegistration::new(ctx.storage(), self.0.as_fd(), direction);
        loop {
            match op(&self.0) {
                Err(Errno::INTR) => {}
                Err(Errno::AGAIN) => registration.wait().await,
                result => return result,
            }
        }
    }
}
