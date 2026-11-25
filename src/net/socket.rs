use core::time::Duration;

use rustix::{
    fd::{AsFd, OwnedFd},
    io::{Errno, FdFlags, fcntl_setfd, ioctl_fionbio},
    net::{
        AddressFamily, RecvFlags, SendFlags, Shutdown, SocketAddrAny, SocketType,
        addr::SocketAddrArg,
        sockopt::{
            set_socket_keepalive, set_socket_reuseaddr, set_tcp_keepcnt, set_tcp_keepidle,
            set_tcp_keepintvl, socket_error,
        },
    },
};

use crate::{
    io::{IoWaiter, IoWaiters},
    selector::{Direction, Selector},
    strace::{syscall, syscall_end, syscall_start},
};

use super::NetError;

const LISTEN_BACKLOG: i32 = 1024;
#[cfg(target_os = "macos")]
const SEND_FLAGS: SendFlags = SendFlags::empty();
#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;

#[must_use]
pub(super) struct Socket<'sel, const FDS: usize> {
    fd: OwnedFd,
    io: &'static IoWaiter,
    io_waiters: &'static IoWaiters<FDS>,
    selector: &'sel Selector,
}

impl<'sel, const FDS: usize> Socket<'sel, FDS> {
    pub(super) fn new(
        io_waiters: &'static IoWaiters<FDS>,
        selector: &'sel Selector,
        family: AddressFamily,
        ty: SocketType,
    ) -> Result<Self, NetError> {
        syscall_start!("socket", family, ty, 0);
        let fd = rustix::net::socket(family, ty, None);
        syscall_end!(fd);
        Self::register(io_waiters, selector, fd?, ty)
    }

    fn register(
        io_waiters: &'static IoWaiters<FDS>,
        selector: &'sel Selector,
        fd: OwnedFd,
        ty: SocketType,
    ) -> Result<Self, NetError> {
        Self::configure(&fd, ty)?;
        let io = io_waiters.register(selector, fd.as_fd())?;
        Ok(Self {
            fd,
            io,
            io_waiters,
            selector,
        })
    }

    pub(super) fn bind<A: SocketAddrArg>(&self, addr: &A) -> rustix::io::Result<()> {
        syscall_start!("bind", self.fd, addr.as_any());
        let result = rustix::net::bind(&self.fd, addr);
        syscall_end!(result);
        result
    }

    pub(super) fn listen(&self) -> rustix::io::Result<()> {
        syscall_start!("listen", self.fd, LISTEN_BACKLOG);
        let result = rustix::net::listen(&self.fd, LISTEN_BACKLOG);
        syscall_end!(result);
        result
    }

    pub(super) fn set_reuseaddr(&self) -> rustix::io::Result<()> {
        syscall_start!("setsockopt", self.fd, "SOL_SOCKET", "SO_REUSEADDR", true);
        let result = set_socket_reuseaddr(&self.fd, true);
        syscall_end!(result);
        result
    }

    pub(super) fn set_keepalive(
        &self,
        idle: Duration,
        interval: Duration,
        count: u32,
    ) -> rustix::io::Result<()> {
        syscall_start!("setsockopt", self.fd, "SOL_SOCKET", "SO_KEEPALIVE", true);
        let result = set_socket_keepalive(&self.fd, true);
        syscall_end!(result);
        result?;

        syscall_start!("setsockopt", self.fd, "IPPROTO_TCP", "TCP_KEEPIDLE", idle);
        let result = set_tcp_keepidle(&self.fd, idle);
        syscall_end!(result);
        result?;

        syscall_start!(
            "setsockopt",
            self.fd,
            "IPPROTO_TCP",
            "TCP_KEEPINTVL",
            interval
        );
        let result = set_tcp_keepintvl(&self.fd, interval);
        syscall_end!(result);
        result?;

        syscall_start!("setsockopt", self.fd, "IPPROTO_TCP", "TCP_KEEPCNT", count);
        let result = set_tcp_keepcnt(&self.fd, count);
        syscall_end!(result);
        result?;

        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let timeout = interval
                .checked_mul(count)
                .and_then(|probes| probes.checked_add(idle))
                .ok_or(Errno::INVAL)?;
            let timeout = u32::try_from(timeout.as_millis()).map_err(|_| Errno::INVAL)?;
            syscall_start!(
                "setsockopt",
                self.fd,
                "IPPROTO_TCP",
                "TCP_USER_TIMEOUT",
                timeout
            );
            let result = rustix::net::sockopt::set_tcp_user_timeout(&self.fd, timeout);
            syscall_end!(result);
            result?;
        }

        Ok(())
    }

    pub(super) fn connect<A: SocketAddrArg>(&self, addr: &A) -> rustix::io::Result<()> {
        syscall_start!("connect", self.fd, addr.as_any());
        let result = rustix::net::connect(&self.fd, addr);
        syscall_end!(result);
        result
    }

    pub(super) async fn connect_stream<A: SocketAddrArg>(
        io_waiters: &'static IoWaiters<FDS>,
        selector: &'sel Selector,
        addr: &A,
    ) -> Result<Self, NetError> {
        let socket = Self::new(
            io_waiters,
            selector,
            addr.as_any().address_family(),
            SocketType::STREAM,
        )?;
        match socket.connect(addr) {
            Ok(()) => return Ok(socket),
            Err(Errno::INPROGRESS | Errno::INTR) => {}
            Err(errno) => return Err(errno.into()),
        }
        socket.io.clear(Direction::Write);
        socket.io.ready(Direction::Write).await;
        syscall_start!("getsockopt", socket.fd, "SOL_SOCKET", "SO_ERROR");
        let result = socket_error(&socket.fd);
        syscall_end!(result; result.ok());
        result??;
        Ok(socket)
    }

    pub(super) async fn accept(&self) -> Result<(Self, Option<SocketAddrAny>), NetError> {
        let (fd, addr) = self
            .retry(Direction::Read, |fd| {
                syscall_start!("accept", fd);
                let result = rustix::net::acceptfrom(fd);
                syscall_end!(result; result.as_ref().ok().and_then(|(_, addr)| addr.as_ref()));
                result
            })
            .await?;
        let socket = Self::register(self.io_waiters, self.selector, fd, SocketType::STREAM)?;
        Ok((socket, addr))
    }

    pub(super) fn send<'a>(
        &'a self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, NetError>> {
        self.retry(Direction::Write, move |fd| {
            syscall_start!("send", fd, buf, buf.len(), SEND_FLAGS);
            let result = rustix::net::send(fd, buf, SEND_FLAGS);
            syscall_end!(result);
            result
        })
    }

    pub(super) fn recv<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, NetError>> {
        self.retry(Direction::Read, move |fd| {
            syscall_start!("recv", fd);
            let result = rustix::net::recv(fd, &mut *buf, RecvFlags::empty());
            syscall_end!(
                result;
                &buf[..result.map_or(0, |(len, _)| len)],
                buf.len(),
                RecvFlags::empty(),
            );
            result.map(|(len, _)| len)
        })
    }

    pub(super) fn send_to<'a, A: SocketAddrArg>(
        &'a self,
        buf: &'a [u8],
        addr: &'a A,
    ) -> impl Future<Output = Result<usize, NetError>> {
        self.retry(Direction::Write, move |fd| {
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

    pub(super) async fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> Result<(usize, Option<SocketAddrAny>), NetError> {
        let (len, _, addr) = self
            .retry(Direction::Read, |fd| {
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
        Ok((len, addr))
    }

    pub(super) fn local_addr<A: TryFrom<SocketAddrAny, Error = Errno>>(
        &self,
    ) -> rustix::io::Result<A> {
        syscall_start!("getsockname", self.fd);
        let addr = rustix::net::getsockname(&self.fd);
        syscall_end!(addr; addr.as_ref().ok());
        addr?.try_into()
    }

    pub(super) fn peer_addr(&self) -> rustix::io::Result<Option<SocketAddrAny>> {
        syscall_start!("getpeername", self.fd);
        let addr = rustix::net::getpeername(&self.fd);
        syscall_end!(addr; addr.as_ref().ok().and_then(Option::as_ref));
        addr
    }

    pub(super) fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        syscall_start!("shutdown", self.fd, how);
        let result = rustix::net::shutdown(&self.fd, how);
        syscall_end!(result);
        result
    }

    fn configure(
        fd: &OwnedFd,
        #[cfg_attr(
            any(target_os = "linux", target_os = "android"),
            expect(unused_variables)
        )]
        ty: SocketType,
    ) -> rustix::io::Result<()> {
        syscall_start!("fcntl", fd, "F_SETFD", FdFlags::CLOEXEC);
        let result = fcntl_setfd(fd, FdFlags::CLOEXEC);
        syscall_end!(result);
        result?;
        syscall_start!("ioctl", fd, "FIONBIO", true);
        let result = ioctl_fionbio(fd, true);
        syscall_end!(result);
        result?;
        #[cfg(target_os = "macos")]
        if ty == SocketType::STREAM {
            syscall_start!("setsockopt", fd, "SOL_SOCKET", "SO_NOSIGPIPE", true);
            let result = rustix::net::sockopt::set_socket_nosigpipe(fd, true);
            syscall_end!(result);
            result?;
        }
        Ok(())
    }

    pub(super) async fn ready(&self, direction: Direction) {
        self.io.ready(direction).await;
    }

    pub(super) fn try_recv(&self, buf: &mut [u8]) -> rustix::io::Result<usize> {
        loop {
            syscall_start!("recv", self.fd);
            let result = rustix::net::recv(&self.fd, &mut *buf, RecvFlags::empty());
            syscall_end!(
                result;
                &buf[..result.map_or(0, |(len, _)| len)],
                buf.len(),
                RecvFlags::empty(),
            );
            match result {
                Ok((len, _)) => return Ok(len),
                Err(Errno::INTR) => {}
                Err(Errno::AGAIN) => {
                    self.io.clear(Direction::Read);
                    return Err(Errno::AGAIN);
                }
                Err(errno) => return Err(errno),
            }
        }
    }

    pub(super) fn try_send(&self, buf: &[u8]) -> rustix::io::Result<usize> {
        loop {
            syscall_start!("send", self.fd, buf, buf.len(), SEND_FLAGS);
            let result = rustix::net::send(&self.fd, buf, SEND_FLAGS);
            syscall_end!(result);
            match result {
                Err(Errno::INTR) => {}
                Err(Errno::AGAIN) => {
                    self.io.clear(Direction::Write);
                    return Err(Errno::AGAIN);
                }
                result => return result,
            }
        }
    }

    async fn retry<T, F: FnMut(&OwnedFd) -> rustix::io::Result<T>>(
        &self,
        direction: Direction,
        mut op: F,
    ) -> Result<T, NetError> {
        Ok(self.io.retry(direction, || op(&self.fd)).await?)
    }
}

impl<const FDS: usize> Drop for Socket<'_, FDS> {
    fn drop(&mut self) {
        self.io_waiters.deregister(self.selector, self.fd.as_fd());
        syscall!("close", self.fd);
    }
}
