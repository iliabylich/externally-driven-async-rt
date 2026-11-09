use rustix::{
    fd::{AsFd, OwnedFd},
    io::{Errno, FdFlags, fcntl_dupfd_cloexec, fcntl_setfd, ioctl_fionbio},
    net::{
        AddressFamily, RecvFlags, SendFlags, Shutdown, SocketType,
        sockopt::{set_socket_reuseaddr, socket_error},
    },
};

use crate::{
    context::Context,
    io::IoRegistration,
    net::address::Address,
    selector::Direction,
    state::State,
    strace::{syscall, syscall_end, syscall_start},
};

const LISTEN_BACKLOG: i32 = 1024;
#[cfg(target_os = "macos")]
const STREAM_SEND_FLAGS: SendFlags = SendFlags::empty();
#[cfg(target_os = "linux")]
const STREAM_SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;

pub(super) struct Socket(OwnedFd);

impl Socket {
    pub(super) fn new(family: AddressFamily, ty: SocketType) -> rustix::io::Result<Self> {
        syscall_start!("socket", family, ty, 0);
        let fd = rustix::net::socket(family, ty, None);
        syscall_end!(fd);
        let socket = Self(fd?);
        socket.configure(ty)?;
        Ok(socket)
    }

    pub(super) fn bind<A: Address>(&self, addr: &A) -> rustix::io::Result<()> {
        syscall_start!("bind", self.0, addr.as_any());
        let result = rustix::net::bind(&self.0, addr);
        syscall_end!(result);
        result
    }

    pub(super) fn listen(&self) -> rustix::io::Result<()> {
        syscall_start!("listen", self.0, LISTEN_BACKLOG);
        let result = rustix::net::listen(&self.0, LISTEN_BACKLOG);
        syscall_end!(result);
        result
    }

    pub(super) fn set_reuseaddr(&self) -> rustix::io::Result<()> {
        syscall_start!("setsockopt", self.0, "SOL_SOCKET", "SO_REUSEADDR", true);
        let result = set_socket_reuseaddr(&self.0, true);
        syscall_end!(result);
        result
    }

    pub(super) fn connect<A: Address>(&self, addr: &A) -> rustix::io::Result<()> {
        syscall_start!("connect", self.0, addr.as_any());
        let result = rustix::net::connect(&self.0, addr);
        syscall_end!(result);
        result
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
        syscall_start!("connect", socket.0, addr.as_any());
        let result = rustix::net::connect(&socket.0, addr);
        syscall_end!(result);
        match result {
            Ok(()) => return Ok(socket),
            Err(Errno::INPROGRESS | Errno::INTR) => {}
            Err(errno) => return Err(errno),
        }
        IoRegistration::new(ctx.storage(), socket.0.as_fd(), Direction::Write)
            .wait()
            .await;
        syscall_start!("getsockopt", socket.0, "SOL_SOCKET", "SO_ERROR");
        let result = socket_error(&socket.0);
        syscall_end!(result; result.ok());
        result??;
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
            .retry(ctx, Direction::Read, |fd| {
                syscall_start!("accept", fd);
                let result = rustix::net::acceptfrom(fd);
                syscall_end!(result; result.as_ref().ok().and_then(|(_, addr)| addr.as_ref()));
                result
            })
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
        self.retry(ctx, Direction::Read, |fd| {
            syscall_start!("read", fd);
            let result = rustix::io::read(fd, &mut *buf);
            syscall_end!(result; &buf[..result.unwrap_or(0)], buf.len());
            result
        })
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
        self.retry(ctx, Direction::Write, |fd| {
            syscall_start!("send", fd, buf, buf.len(), STREAM_SEND_FLAGS);
            let result = rustix::net::send(fd, buf, STREAM_SEND_FLAGS);
            syscall_end!(result);
            result
        })
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
            syscall_start!("send", fd, buf, buf.len(), SendFlags::empty());
            let result = rustix::net::send(fd, buf, SendFlags::empty());
            syscall_end!(result);
            result
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
                syscall_start!("recv", fd);
                let result = rustix::net::recv(fd, &mut *buf, RecvFlags::empty());
                syscall_end!(
                    result;
                    &buf[..result.map_or(0, |(len, _)| len)],
                    buf.len(),
                    RecvFlags::empty(),
                );
                result
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
            syscall_start!(
                "sendto",
                fd,
                buf,
                buf.len(),
                SendFlags::empty(),
                addr.as_any()
            );
            let result = rustix::net::sendto(fd, buf, SendFlags::empty(), addr);
            syscall_end!(result);
            result
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
                syscall_start!("recvfrom", fd);
                let result = rustix::net::recvfrom(fd, &mut *buf, RecvFlags::empty());
                syscall_end!(
                    result;
                    &buf[..result.as_ref().map_or(0, |(len, _, _)| *len)],
                    buf.len(),
                    RecvFlags::empty(),
                    result.as_ref().ok().and_then(|(_, _, addr)| addr.as_ref()),
                );
                result
            })
            .await?;
        Ok((len, A::from_any(addr)?))
    }

    pub(super) fn local_addr<A: Address>(&self) -> rustix::io::Result<A> {
        syscall_start!("getsockname", self.0);
        let addr = rustix::net::getsockname(&self.0);
        syscall_end!(addr; addr.as_ref().ok());
        A::from_any(Some(addr?))
    }

    pub(super) fn peer_addr<A: Address>(&self) -> rustix::io::Result<A> {
        syscall_start!("getpeername", self.0);
        let addr = rustix::net::getpeername(&self.0);
        syscall_end!(addr; addr.as_ref().ok().and_then(Option::as_ref));
        A::from_any(addr?)
    }

    pub(super) fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        syscall_start!("shutdown", self.0, how);
        let result = rustix::net::shutdown(&self.0, how);
        syscall_end!(result);
        result
    }

    pub(super) fn try_clone(&self) -> rustix::io::Result<Self> {
        syscall_start!("fcntl", self.0, "F_DUPFD_CLOEXEC", 0);
        let fd = fcntl_dupfd_cloexec(&self.0, 0);
        syscall_end!(fd);
        Ok(Self(fd?))
    }

    fn configure(
        &self,
        #[cfg_attr(target_os = "linux", expect(unused_variables))] ty: SocketType,
    ) -> rustix::io::Result<()> {
        syscall_start!("fcntl", self.0, "F_SETFD", FdFlags::CLOEXEC);
        let result = fcntl_setfd(&self.0, FdFlags::CLOEXEC);
        syscall_end!(result);
        result?;
        syscall_start!("ioctl", self.0, "FIONBIO", true);
        let result = ioctl_fionbio(&self.0, true);
        syscall_end!(result);
        result?;
        #[cfg(target_os = "macos")]
        if ty == SocketType::STREAM {
            syscall_start!("setsockopt", self.0, "SOL_SOCKET", "SO_NOSIGPIPE", true);
            let result = rustix::net::sockopt::set_socket_nosigpipe(&self.0, true);
            syscall_end!(result);
            result?;
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

impl Drop for Socket {
    fn drop(&mut self) {
        syscall!("close", self.0);
    }
}
