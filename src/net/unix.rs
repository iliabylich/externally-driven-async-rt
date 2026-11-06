use rustix::fd::OwnedFd;
use rustix::net::{AddressFamily, Shutdown, SocketAddrUnix, SocketType};

use super::{ReadHalf, WriteHalf};
use crate::context::Context;

pub struct UnixListener {
    fd: OwnedFd,
}

impl UnixListener {
    pub fn bind(addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let fd = super::bind(AddressFamily::UNIX, SocketType::STREAM, addr)?;
        super::listen(&fd)?;
        Ok(Self { fd })
    }

    pub async fn accept<E>(
        &mut self,
        ctx: &Context<E>,
    ) -> rustix::io::Result<(UnixStream, SocketAddrUnix)> {
        let (fd, addr) = super::accept(ctx, &self.fd).await?;
        Ok((UnixStream { fd }, addr))
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        super::local_addr(&self.fd)
    }
}

pub struct UnixStream {
    fd: OwnedFd,
}

impl UnixStream {
    pub async fn connect<E>(ctx: &Context<E>, addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let fd = super::connect(ctx, AddressFamily::UNIX, addr).await?;
        Ok(Self { fd })
    }

    pub async fn read<E>(&mut self, ctx: &Context<E>, buf: &mut [u8]) -> rustix::io::Result<usize> {
        super::read(ctx, &self.fd, buf).await
    }

    pub async fn write<E>(&mut self, ctx: &Context<E>, buf: &[u8]) -> rustix::io::Result<usize> {
        super::write(ctx, &self.fd, buf).await
    }

    pub fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        rustix::net::shutdown(&self.fd, how)
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        super::local_addr(&self.fd)
    }

    pub fn peer_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        super::peer_addr(&self.fd)
    }

    pub fn into_split(self) -> rustix::io::Result<(ReadHalf, WriteHalf)> {
        super::split(self.fd)
    }
}

pub struct UnixDatagram {
    fd: OwnedFd,
}

impl UnixDatagram {
    pub fn bind(addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let fd = super::bind(AddressFamily::UNIX, SocketType::DGRAM, addr)?;
        Ok(Self { fd })
    }

    pub fn unbound() -> rustix::io::Result<Self> {
        let fd = super::socket(AddressFamily::UNIX, SocketType::DGRAM)?;
        Ok(Self { fd })
    }

    pub async fn send_to<E>(
        &mut self,
        ctx: &Context<E>,
        buf: &[u8],
        addr: &SocketAddrUnix,
    ) -> rustix::io::Result<usize> {
        super::send_to(ctx, &self.fd, buf, addr).await
    }

    pub async fn recv_from<E>(
        &mut self,
        ctx: &Context<E>,
        buf: &mut [u8],
    ) -> rustix::io::Result<(usize, SocketAddrUnix)> {
        super::recv_from(ctx, &self.fd, buf).await
    }

    pub fn connect(&self, addr: &SocketAddrUnix) -> rustix::io::Result<()> {
        rustix::net::connect(&self.fd, addr)
    }

    pub async fn send<E>(&mut self, ctx: &Context<E>, buf: &[u8]) -> rustix::io::Result<usize> {
        super::send(ctx, &self.fd, buf).await
    }

    pub async fn recv<E>(&mut self, ctx: &Context<E>, buf: &mut [u8]) -> rustix::io::Result<usize> {
        super::recv(ctx, &self.fd, buf).await
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        super::local_addr(&self.fd)
    }
}

#[cfg(test)]
mod tests {
    use core::future::pending;
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::thread;

    use super::*;
    use crate::EventLoop;
    use crate::test_utils::{Log, run_until_events};

    fn temp_path(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("edar-{}-{name}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn addr(path: &Path) -> SocketAddrUnix {
        SocketAddrUnix::new(path).unwrap()
    }

    fn path_of(addr: &SocketAddrUnix) -> &[u8] {
        addr.path_bytes().unwrap_or_default()
    }

    #[test]
    fn stream_echo_with_a_std_server() {
        let path = temp_path("stream-echo");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0; 64];
            loop {
                let len = stream.read(&mut buf).unwrap();
                if len == 0 {
                    return;
                }
                stream.write_all(&buf[..len]).unwrap();
            }
        });
        let server_addr = addr(&path);
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let mut stream = UnixStream::connect(ctx, &server_addr).await.unwrap();
            assert_eq!(path_of(&stream.peer_addr().unwrap()), path_of(&server_addr));
            assert!(path_of(&stream.local_addr().unwrap()).is_empty());
            stream.write(ctx, b"hello").await.unwrap();
            let mut buf = [0; 5];
            let mut len = 0;
            while len < 5 {
                len += stream.read(ctx, &mut buf[len..]).await.unwrap();
            }
            assert_eq!(&buf, b"hello");
            stream.shutdown(Shutdown::Write).unwrap();
            assert_eq!(stream.read(ctx, &mut buf).await.unwrap(), 0);
            ctx.dispatch(1);
            pending::<()>().await;
        })
        .unwrap();
        run_until_events(&mut event_loop, 1);
        server.join().unwrap();
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn listener_accepts_a_std_client() {
        let path = temp_path("listener");
        let (ready_tx, ready_rx) = mpsc::channel();
        let client_path = path.clone();
        let client = thread::spawn(move || {
            ready_rx.recv().unwrap();
            let mut stream = std::os::unix::net::UnixStream::connect(&client_path).unwrap();
            stream.write_all(b"ping").unwrap();
            let mut buf = [0; 4];
            stream.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"pong");
        });
        let listener_addr = addr(&path);
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let mut listener = UnixListener::bind(&listener_addr).unwrap();
            assert_eq!(
                path_of(&listener.local_addr().unwrap()),
                path_of(&listener_addr)
            );
            ready_tx.send(()).unwrap();
            let (mut stream, peer) = listener.accept(ctx).await.unwrap();
            assert!(path_of(&peer).is_empty());
            let mut buf = [0; 4];
            let mut len = 0;
            while len < 4 {
                len += stream.read(ctx, &mut buf[len..]).await.unwrap();
            }
            assert_eq!(&buf, b"ping");
            stream.write(ctx, b"pong").await.unwrap();
            ctx.dispatch(1);
            pending::<()>().await;
        })
        .unwrap();
        run_until_events(&mut event_loop, 1);
        client.join().unwrap();
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn datagram_round_trip_with_a_std_peer() {
        let ours = temp_path("dgram-ours");
        let theirs = temp_path("dgram-theirs");
        let peer_socket = std::os::unix::net::UnixDatagram::bind(&theirs).unwrap();
        let expected_from = ours.clone();
        let peer = thread::spawn(move || {
            let mut buf = [0; 64];
            for _ in 0..2 {
                let (len, from) = peer_socket.recv_from(&mut buf).unwrap();
                assert_eq!(from.as_pathname(), Some(expected_from.as_path()));
                peer_socket
                    .send_to(&buf[..len], from.as_pathname().unwrap())
                    .unwrap();
            }
        });
        let (ours_addr, theirs_addr) = (addr(&ours), addr(&theirs));
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let mut socket = UnixDatagram::bind(&ours_addr).unwrap();
            assert_eq!(path_of(&socket.local_addr().unwrap()), path_of(&ours_addr));
            let mut buf = [0; 64];

            socket.send_to(ctx, b"ping", &theirs_addr).await.unwrap();
            let (len, from) = socket.recv_from(ctx, &mut buf).await.unwrap();
            assert_eq!(&buf[..len], b"ping");
            assert_eq!(path_of(&from), path_of(&theirs_addr));

            socket.connect(&theirs_addr).unwrap();
            socket.send(ctx, b"hello").await.unwrap();
            let len = socket.recv(ctx, &mut buf).await.unwrap();
            assert_eq!(&buf[..len], b"hello");

            ctx.dispatch(1);
            pending::<()>().await;
        })
        .unwrap();
        run_until_events(&mut event_loop, 1);
        peer.join().unwrap();
        std::fs::remove_file(&ours).unwrap();
        std::fs::remove_file(&theirs).unwrap();
    }

    #[test]
    fn unbound_datagram_sender_has_an_unnamed_address() {
        let path = temp_path("dgram-unbound");
        let bound_addr = addr(&path);
        let mut event_loop = EventLoop::new(Log::default(), async move |ctx| {
            let mut bound = UnixDatagram::bind(&bound_addr).unwrap();
            let mut unbound = UnixDatagram::unbound().unwrap();
            unbound.send_to(ctx, b"hi", &bound_addr).await.unwrap();
            let mut buf = [0; 8];
            let (len, from) = bound.recv_from(ctx, &mut buf).await.unwrap();
            assert_eq!(&buf[..len], b"hi");
            assert!(path_of(&from).is_empty());
            ctx.dispatch(1);
            pending::<()>().await;
        })
        .unwrap();
        run_until_events(&mut event_loop, 1);
        std::fs::remove_file(&path).unwrap();
    }
}
