use core::cell::Cell;

use rustix::{
    fd::{AsFd, OwnedFd},
    io::{Errno, FdFlags, fcntl_setfd, ioctl_fionbio},
    net::{
        AddressFamily, RecvFlags, SendFlags, Shutdown, SocketType,
        sockopt::{set_socket_reuseaddr, socket_error},
    },
};

use crate::{
    context::Context,
    io::{IoIndex, IoRegistration},
    net::{address::Address, retry::Retry},
    selector::Direction,
    state::State,
    strace::{syscall, syscall_end, syscall_start},
};

const LISTEN_BACKLOG: i32 = 1024;
#[cfg(target_os = "macos")]
const STREAM_SEND_FLAGS: SendFlags = SendFlags::empty();
#[cfg(target_os = "linux")]
const STREAM_SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;

pub(crate) struct Socket(OwnedFd, Cell<Option<IoIndex>>);

impl Socket {
    pub(crate) fn new(family: AddressFamily, ty: SocketType) -> rustix::io::Result<Self> {
        syscall_start!("socket", family, ty, 0);
        let fd = rustix::net::socket(family, ty, None);
        syscall_end!(fd);
        let socket = Self(fd?, Cell::new(None));
        socket.configure(ty)?;
        Ok(socket)
    }

    pub(crate) fn bind<A: Address>(&self, addr: &A) -> rustix::io::Result<()> {
        syscall_start!("bind", self.0, addr.as_any());
        let result = rustix::net::bind(&self.0, addr);
        syscall_end!(result);
        result
    }

    pub(crate) fn listen(&self) -> rustix::io::Result<()> {
        syscall_start!("listen", self.0, LISTEN_BACKLOG);
        let result = rustix::net::listen(&self.0, LISTEN_BACKLOG);
        syscall_end!(result);
        result
    }

    pub(crate) fn set_reuseaddr(&self) -> rustix::io::Result<()> {
        syscall_start!("setsockopt", self.0, "SOL_SOCKET", "SO_REUSEADDR", true);
        let result = set_socket_reuseaddr(&self.0, true);
        syscall_end!(result);
        result
    }

    pub(crate) fn connect<A: Address>(&self, addr: &A) -> rustix::io::Result<()> {
        syscall_start!("connect", self.0, addr.as_any());
        let result = rustix::net::connect(&self.0, addr);
        syscall_end!(result);
        result
    }

    pub(crate) async fn connect_stream<
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
        IoRegistration::new(ctx.storage(), socket.0.as_fd(), &socket.1, Direction::Write)
            .wait()
            .await;
        syscall_start!("getsockopt", socket.0, "SOL_SOCKET", "SO_ERROR");
        let result = socket_error(&socket.0);
        syscall_end!(result; result.ok());
        result??;
        Ok(socket)
    }

    pub(crate) async fn accept<
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
        let socket = Self(fd, Cell::new(None));
        socket.configure(SocketType::STREAM)?;
        Ok((socket, A::from_any(addr)?))
    }

    pub(crate) fn read<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a self,
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
        self.retry(ctx, Direction::Read, move |fd| {
            syscall_start!("read", fd);
            let result = rustix::io::read(fd, &mut *buf);
            syscall_end!(result; &buf[..result.unwrap_or(0)], buf.len());
            result
        })
    }

    pub(crate) fn write<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a self,
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
        self.retry(ctx, Direction::Write, move |fd| {
            syscall_start!("send", fd, buf, buf.len(), STREAM_SEND_FLAGS);
            let result = rustix::net::send(fd, buf, STREAM_SEND_FLAGS);
            syscall_end!(result);
            result
        })
    }

    pub(crate) fn send<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &'a self,
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
        self.retry(ctx, Direction::Write, move |fd| {
            syscall_start!("send", fd, buf, buf.len(), SendFlags::empty());
            let result = rustix::net::send(fd, buf, SendFlags::empty());
            syscall_end!(result);
            result
        })
    }

    pub(crate) async fn recv<
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

    pub(crate) fn send_to<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
        A: Address,
    >(
        &'a self,
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
        addr: &'a A,
    ) -> impl Future<Output = rustix::io::Result<usize>> {
        self.retry(ctx, Direction::Write, move |fd| {
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
    }

    pub(crate) async fn recv_from<
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

    pub(crate) fn local_addr<A: Address>(&self) -> rustix::io::Result<A> {
        syscall_start!("getsockname", self.0);
        let addr = rustix::net::getsockname(&self.0);
        syscall_end!(addr; addr.as_ref().ok());
        A::from_any(Some(addr?))
    }

    pub(crate) fn peer_addr<A: Address>(&self) -> rustix::io::Result<A> {
        syscall_start!("getpeername", self.0);
        let addr = rustix::net::getpeername(&self.0);
        syscall_end!(addr; addr.as_ref().ok().and_then(Option::as_ref));
        A::from_any(addr?)
    }

    pub(crate) fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        syscall_start!("shutdown", self.0, how);
        let result = rustix::net::shutdown(&self.0, how);
        syscall_end!(result);
        result
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

    fn retry<
        'a,
        'ctx,
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
        T,
        F: FnMut(&OwnedFd) -> rustix::io::Result<T> + Unpin,
    >(
        &'a self,
        ctx: &'a Context<
            'ctx,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        direction: Direction,
        op: F,
    ) -> Retry<
        'a,
        'ctx,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        F,
    > {
        Retry::new(ctx, &self.0, &self.1, direction, op)
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        syscall!("close", self.0);
    }
}
