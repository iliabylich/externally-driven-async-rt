mod halves;
mod tcp;
mod udp;
mod unix;

use core::net::SocketAddr;

use rustix::fd::{AsFd, OwnedFd};
use rustix::io::{Errno, FdFlags, fcntl_setfd, ioctl_fionbio};
use rustix::net::addr::SocketAddrArg;
use rustix::net::sockopt::{set_socket_nosigpipe, socket_error};
use rustix::net::{AddressFamily, RecvFlags, SendFlags, SocketAddrAny, SocketAddrUnix, SocketType};

use crate::context::Context;
use crate::inner::Direction;
use crate::io::{IoRegistration, io_loop};

pub use halves::{ReadHalf, WriteHalf};
pub use tcp::{TcpListener, TcpStream};
pub use udp::UdpSocket;
pub use unix::{UnixDatagram, UnixListener, UnixStream};

const LISTEN_BACKLOG: i32 = 1024;

trait Address: SocketAddrArg + Sized {
    fn from_any(addr: Option<SocketAddrAny>) -> rustix::io::Result<Self>;
}

impl Address for SocketAddr {
    fn from_any(addr: Option<SocketAddrAny>) -> rustix::io::Result<Self> {
        addr.ok_or(Errno::AFNOSUPPORT)?.try_into()
    }
}

impl Address for SocketAddrUnix {
    fn from_any(addr: Option<SocketAddrAny>) -> rustix::io::Result<Self> {
        match addr {
            Some(addr) if !is_unnamed_unix(&addr) => addr.try_into(),
            _ => SocketAddrUnix::new(""),
        }
    }
}

// macOS reports an unnamed Unix socket as a 16-byte address with an all-zero `sun_path`, which
// rustix (1.1.6) would decode as a 13-byte path of NULs (and trips a `debug_assert`), so unnamed
// addresses are detected here, by an empty `sun_path`, before converting.
fn is_unnamed_unix(addr: &SocketAddrAny) -> bool {
    const SUN_PATH_OFFSET: usize = 2; // after `sun_len: u8` and `sun_family: u8`
    // SAFETY: `as_ptr` points to the `addr_len()` initialized bytes of the address.
    let bytes = unsafe {
        core::slice::from_raw_parts(addr.as_ptr().cast::<u8>(), addr.addr_len() as usize)
    };
    bytes.get(SUN_PATH_OFFSET).is_none_or(|byte| *byte == 0)
}

fn ip_family(addr: &SocketAddr) -> AddressFamily {
    match addr {
        SocketAddr::V4(_) => AddressFamily::INET,
        SocketAddr::V6(_) => AddressFamily::INET6,
    }
}

fn socket(family: AddressFamily, ty: SocketType) -> rustix::io::Result<OwnedFd> {
    let fd = rustix::net::socket(family, ty, None)?;
    configure(&fd, ty)?;
    Ok(fd)
}

// macOS has neither `SOCK_CLOEXEC`/`SOCK_NONBLOCK` nor `MSG_NOSIGNAL`, so these are set on the fd.
// Without `SO_NOSIGPIPE` a write to a closed peer raises SIGPIPE, which kills a host process that
// doesn't ignore it.
fn configure(fd: &OwnedFd, ty: SocketType) -> rustix::io::Result<()> {
    fcntl_setfd(fd, FdFlags::CLOEXEC)?;
    ioctl_fionbio(fd, true)?;
    if ty == SocketType::STREAM {
        set_socket_nosigpipe(fd, true)?;
    }
    Ok(())
}

fn bind<A: Address>(
    family: AddressFamily,
    ty: SocketType,
    addr: &A,
) -> rustix::io::Result<OwnedFd> {
    let fd = socket(family, ty)?;
    rustix::net::bind(&fd, addr)?;
    Ok(fd)
}

fn listen(fd: &OwnedFd) -> rustix::io::Result<()> {
    rustix::net::listen(fd, LISTEN_BACKLOG)
}

fn local_addr<A: Address>(fd: &OwnedFd) -> rustix::io::Result<A> {
    A::from_any(Some(rustix::net::getsockname(fd)?))
}

fn peer_addr<A: Address>(fd: &OwnedFd) -> rustix::io::Result<A> {
    A::from_any(rustix::net::getpeername(fd)?)
}

async fn connect<E, A: Address>(
    ctx: &Context<E>,
    family: AddressFamily,
    addr: &A,
) -> rustix::io::Result<OwnedFd> {
    let fd = socket(family, SocketType::STREAM)?;
    match rustix::net::connect(&fd, addr) {
        Ok(()) => return Ok(fd),
        // An interrupted connect keeps going in the background, just like `EINPROGRESS`.
        Err(Errno::INPROGRESS | Errno::INTR) => {}
        Err(errno) => return Err(errno),
    }
    IoRegistration::new(ctx.inner(), fd.as_fd())
        .wait(Direction::Write)
        .await;
    socket_error(&fd)??;
    Ok(fd)
}

async fn accept<E, A: Address>(ctx: &Context<E>, fd: &OwnedFd) -> rustix::io::Result<(OwnedFd, A)> {
    let (stream, addr) = io_loop!(
        ctx,
        fd.as_fd(),
        Direction::Read,
        rustix::net::acceptfrom(fd)
    )?;
    configure(&stream, SocketType::STREAM)?;
    Ok((stream, A::from_any(addr)?))
}

async fn read<E>(ctx: &Context<E>, fd: &OwnedFd, buf: &mut [u8]) -> rustix::io::Result<usize> {
    io_loop!(
        ctx,
        fd.as_fd(),
        Direction::Read,
        rustix::io::read(fd, &mut *buf)
    )
}

async fn write<E>(ctx: &Context<E>, fd: &OwnedFd, buf: &[u8]) -> rustix::io::Result<usize> {
    io_loop!(
        ctx,
        fd.as_fd(),
        Direction::Write,
        rustix::io::write(fd, buf)
    )
}

async fn send<E>(ctx: &Context<E>, fd: &OwnedFd, buf: &[u8]) -> rustix::io::Result<usize> {
    io_loop!(
        ctx,
        fd.as_fd(),
        Direction::Write,
        rustix::net::send(fd, buf, SendFlags::empty())
    )
}

async fn recv<E>(ctx: &Context<E>, fd: &OwnedFd, buf: &mut [u8]) -> rustix::io::Result<usize> {
    let (len, _) = io_loop!(
        ctx,
        fd.as_fd(),
        Direction::Read,
        rustix::net::recv(fd, &mut *buf, RecvFlags::empty())
    )?;
    Ok(len)
}

async fn send_to<E, A: Address>(
    ctx: &Context<E>,
    fd: &OwnedFd,
    buf: &[u8],
    addr: &A,
) -> rustix::io::Result<usize> {
    io_loop!(
        ctx,
        fd.as_fd(),
        Direction::Write,
        rustix::net::sendto(fd, buf, SendFlags::empty(), addr)
    )
}

async fn recv_from<E, A: Address>(
    ctx: &Context<E>,
    fd: &OwnedFd,
    buf: &mut [u8],
) -> rustix::io::Result<(usize, A)> {
    let (len, _, addr) = io_loop!(
        ctx,
        fd.as_fd(),
        Direction::Read,
        rustix::net::recvfrom(fd, &mut *buf, RecvFlags::empty())
    )?;
    Ok((len, A::from_any(addr)?))
}

fn split(fd: OwnedFd) -> rustix::io::Result<(ReadHalf, WriteHalf)> {
    let write = rustix::io::fcntl_dupfd_cloexec(&fd, 0)?;
    Ok((ReadHalf::new(fd), WriteHalf::new(write)))
}
