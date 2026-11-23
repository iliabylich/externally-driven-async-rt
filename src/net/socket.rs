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
    NotEnoughResources,
    io::IoWaiters,
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

    pub(super) fn bind<A: SocketAddrArg>(&self, addr: &A) -> rustix::io::Result<()> {
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

    pub(super) fn set_keepalive(
        &self,
        idle: Duration,
        interval: Duration,
        count: u32,
    ) -> rustix::io::Result<()> {
        syscall_start!("setsockopt", self.0, "SOL_SOCKET", "SO_KEEPALIVE", true);
        let result = set_socket_keepalive(&self.0, true);
        syscall_end!(result);
        result?;

        syscall_start!("setsockopt", self.0, "IPPROTO_TCP", "TCP_KEEPIDLE", idle);
        let result = set_tcp_keepidle(&self.0, idle);
        syscall_end!(result);
        result?;

        syscall_start!(
            "setsockopt",
            self.0,
            "IPPROTO_TCP",
            "TCP_KEEPINTVL",
            interval
        );
        let result = set_tcp_keepintvl(&self.0, interval);
        syscall_end!(result);
        result?;

        syscall_start!("setsockopt", self.0, "IPPROTO_TCP", "TCP_KEEPCNT", count);
        let result = set_tcp_keepcnt(&self.0, count);
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
                self.0,
                "IPPROTO_TCP",
                "TCP_USER_TIMEOUT",
                timeout
            );
            let result = rustix::net::sockopt::set_tcp_user_timeout(&self.0, timeout);
            syscall_end!(result);
            result?;
        }

        Ok(())
    }

    pub(super) fn connect<A: SocketAddrArg>(&self, addr: &A) -> rustix::io::Result<()> {
        syscall_start!("connect", self.0, addr.as_any());
        let result = rustix::net::connect(&self.0, addr);
        syscall_end!(result);
        result
    }

    pub(super) async fn connect_stream<const FDS: usize, A: SocketAddrArg>(
        io_waiters: &IoWaiters<FDS>,
        selector: &Selector,
        addr: &A,
    ) -> Result<Self, NetError> {
        let socket = Self::new(addr.as_any().address_family(), SocketType::STREAM)?;
        match socket.connect(addr) {
            Ok(()) => return Ok(socket),
            Err(Errno::INPROGRESS | Errno::INTR) => {}
            Err(errno) => return Err(errno.into()),
        }
        io_waiters
            .ready(selector, socket.0.as_fd(), Direction::Write)
            .await?;
        syscall_start!("getsockopt", socket.0, "SOL_SOCKET", "SO_ERROR");
        let result = socket_error(&socket.0);
        syscall_end!(result; result.ok());
        result??;
        Ok(socket)
    }

    pub(super) async fn accept<const FDS: usize>(
        &self,
        io_waiters: &IoWaiters<FDS>,
        selector: &Selector,
    ) -> Result<(Self, Option<SocketAddrAny>), NetError> {
        let (fd, addr) = self
            .retry(io_waiters, selector, Direction::Read, |fd| {
                syscall_start!("accept", fd);
                let result = rustix::net::acceptfrom(fd);
                syscall_end!(result; result.as_ref().ok().and_then(|(_, addr)| addr.as_ref()));
                result
            })
            .await?;
        let socket = Self(fd);
        socket.configure(SocketType::STREAM)?;
        Ok((socket, addr))
    }

    pub(super) fn send<'a, const FDS: usize>(
        &'a self,
        io_waiters: &'a IoWaiters<FDS>,
        selector: &'a Selector,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, NetError>> {
        self.retry(io_waiters, selector, Direction::Write, move |fd| {
            syscall_start!("send", fd, buf, buf.len(), SEND_FLAGS);
            let result = rustix::net::send(fd, buf, SEND_FLAGS);
            syscall_end!(result);
            result
        })
    }

    pub(super) fn recv<'a, const FDS: usize>(
        &'a self,
        io_waiters: &'a IoWaiters<FDS>,
        selector: &'a Selector,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, NetError>> {
        self.retry(io_waiters, selector, Direction::Read, move |fd| {
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

    pub(super) fn send_to<'a, const FDS: usize, A: SocketAddrArg>(
        &'a self,
        io_waiters: &'a IoWaiters<FDS>,
        selector: &'a Selector,
        buf: &'a [u8],
        addr: &'a A,
    ) -> impl Future<Output = Result<usize, NetError>> {
        self.retry(io_waiters, selector, Direction::Write, move |fd| {
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

    pub(super) async fn recv_from<const FDS: usize>(
        &self,
        io_waiters: &IoWaiters<FDS>,
        selector: &Selector,
        buf: &mut [u8],
    ) -> Result<(usize, Option<SocketAddrAny>), NetError> {
        let (len, _, addr) = self
            .retry(io_waiters, selector, Direction::Read, |fd| {
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
        syscall_start!("getsockname", self.0);
        let addr = rustix::net::getsockname(&self.0);
        syscall_end!(addr; addr.as_ref().ok());
        addr?.try_into()
    }

    pub(super) fn peer_addr(&self) -> rustix::io::Result<Option<SocketAddrAny>> {
        syscall_start!("getpeername", self.0);
        let addr = rustix::net::getpeername(&self.0);
        syscall_end!(addr; addr.as_ref().ok().and_then(Option::as_ref));
        addr
    }

    pub(super) fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        syscall_start!("shutdown", self.0, how);
        let result = rustix::net::shutdown(&self.0, how);
        syscall_end!(result);
        result
    }

    fn configure(
        &self,
        #[cfg_attr(
            any(target_os = "linux", target_os = "android"),
            expect(unused_variables)
        )]
        ty: SocketType,
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

    pub(super) async fn ready<const FDS: usize>(
        &self,
        io_waiters: &IoWaiters<FDS>,
        selector: &Selector,
        direction: Direction,
    ) -> Result<(), NotEnoughResources> {
        io_waiters.ready(selector, self.0.as_fd(), direction).await
    }

    pub(super) fn try_recv(&self, buf: &mut [u8]) -> rustix::io::Result<usize> {
        loop {
            syscall_start!("recv", self.0);
            let result = rustix::net::recv(&self.0, &mut *buf, RecvFlags::empty());
            syscall_end!(
                result;
                &buf[..result.map_or(0, |(len, _)| len)],
                buf.len(),
                RecvFlags::empty(),
            );
            match result {
                Ok((len, _)) => return Ok(len),
                Err(Errno::INTR) => {}
                Err(errno) => return Err(errno),
            }
        }
    }

    pub(super) fn try_send(&self, buf: &[u8]) -> rustix::io::Result<usize> {
        loop {
            syscall_start!("send", self.0, buf, buf.len(), SEND_FLAGS);
            let result = rustix::net::send(&self.0, buf, SEND_FLAGS);
            syscall_end!(result);
            if result != Err(Errno::INTR) {
                return result;
            }
        }
    }

    async fn retry<const FDS: usize, T, F: FnMut(&OwnedFd) -> rustix::io::Result<T>>(
        &self,
        io_waiters: &IoWaiters<FDS>,
        selector: &Selector,
        direction: Direction,
        mut op: F,
    ) -> Result<T, NetError> {
        let result = io_waiters
            .retry(selector, self.0.as_fd(), direction, || op(&self.0))
            .await?;
        Ok(result?)
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        syscall!("close", self.0);
    }
}
