use core::net::SocketAddr;

use rustix::fd::OwnedFd;
use rustix::net::sockopt::set_socket_reuseaddr;
use rustix::net::{Shutdown, SocketType};

use super::{ReadHalf, WriteHalf};
use crate::context::Context;
use crate::state::State;

pub struct TcpListener {
    fd: OwnedFd,
}

impl TcpListener {
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let fd = super::socket(super::ip_family(&addr), SocketType::STREAM)?;
        set_socket_reuseaddr(&fd, true)?;
        rustix::net::bind(&fd, &addr)?;
        super::listen(&fd)?;
        Ok(Self { fd })
    }

    pub async fn accept<S: State>(
        &mut self,
        ctx: &Context<S>,
    ) -> rustix::io::Result<(TcpStream, SocketAddr)> {
        let (fd, addr) = super::accept(ctx, &self.fd).await?;
        Ok((TcpStream { fd }, addr))
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        super::local_addr(&self.fd)
    }
}

pub struct TcpStream {
    fd: OwnedFd,
}

impl TcpStream {
    pub async fn connect<S: State>(ctx: &Context<S>, addr: SocketAddr) -> rustix::io::Result<Self> {
        let fd = super::connect(ctx, super::ip_family(&addr), &addr).await?;
        Ok(Self { fd })
    }

    pub async fn read<S: State>(
        &mut self,
        ctx: &Context<S>,
        buf: &mut [u8],
    ) -> rustix::io::Result<usize> {
        super::read(ctx, &self.fd, buf).await
    }

    pub async fn write<S: State>(
        &mut self,
        ctx: &Context<S>,
        buf: &[u8],
    ) -> rustix::io::Result<usize> {
        super::write(ctx, &self.fd, buf).await
    }

    pub fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        rustix::net::shutdown(&self.fd, how)
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        super::local_addr(&self.fd)
    }

    pub fn peer_addr(&self) -> rustix::io::Result<SocketAddr> {
        super::peer_addr(&self.fd)
    }

    pub fn into_split(self) -> rustix::io::Result<(ReadHalf, WriteHalf)> {
        super::split(self.fd)
    }
}

#[cfg(test)]
mod tests {
    use core::future::pending;
    use std::io::{Read, Write};
    use std::sync::mpsc;
    use std::thread;

    use rustix::io::Errno;

    use super::*;
    use crate::EventLoop;
    use crate::test_utils::{Log, TEST_MAX_IO, TEST_MAX_TASKS, TEST_MAX_TIMERS, run_until_events};

    fn std_echo_server() -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0; 4096];
            loop {
                let len = stream.read(&mut buf).unwrap();
                if len == 0 {
                    return;
                }
                stream.write_all(&buf[..len]).unwrap();
            }
        });
        (addr, server)
    }

    #[test]
    fn echo_with_a_std_server() {
        let (addr, server) = std_echo_server();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let mut stream = TcpStream::connect(ctx, addr).await.unwrap();
                assert_eq!(stream.peer_addr().unwrap(), addr);
                assert!(stream.local_addr().unwrap().ip().is_loopback());
                assert_eq!(stream.write(ctx, b"hello").await.unwrap(), 5);
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
            },
        )
        .unwrap();
        run_until_events(&mut event_loop, 1);
        server.join().unwrap();
    }

    #[test]
    fn listener_accepts_a_std_client() {
        let (addr_tx, addr_rx) = mpsc::channel();
        let client = thread::spawn(move || {
            let mut stream = std::net::TcpStream::connect(addr_rx.recv().unwrap()).unwrap();
            stream.write_all(b"ping").unwrap();
            let mut buf = [0; 4];
            stream.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"pong");
        });
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let mut listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                let (mut stream, peer) = listener.accept(ctx).await.unwrap();
                assert_eq!(stream.peer_addr().unwrap(), peer);
                let mut buf = [0; 4];
                let mut len = 0;
                while len < 4 {
                    len += stream.read(ctx, &mut buf[len..]).await.unwrap();
                }
                assert_eq!(&buf, b"ping");
                stream.write(ctx, b"pong").await.unwrap();
                ctx.dispatch(1);
                pending::<()>().await;
            },
        )
        .unwrap();
        run_until_events(&mut event_loop, 1);
        client.join().unwrap();
    }

    #[test]
    fn connect_to_a_closed_port_fails() {
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let errno = TcpStream::connect(ctx, addr).await.err().unwrap();
                ctx.dispatch(errno.raw_os_error() as u32);
                pending::<()>().await;
            },
        )
        .unwrap();
        run_until_events(&mut event_loop, 1);
        assert_eq!(
            event_loop.state_mut().0,
            vec![Errno::CONNREFUSED.raw_os_error() as u32]
        );
    }

    #[test]
    fn halves_are_used_from_two_tasks() {
        const LEN: usize = 1 << 20;
        let (addr, server) = std_echo_server();
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let stream = TcpStream::connect(ctx, addr).await.unwrap();
                let (mut read, mut write) = stream.into_split().unwrap();
                let reader = ctx.spawn(async move |ctx| {
                    let mut buf = [0; 4096];
                    let mut received = Vec::new();
                    loop {
                        let len = read.read(ctx, &mut buf).await.unwrap();
                        if len == 0 {
                            break;
                        }
                        received.extend_from_slice(&buf[..len]);
                    }
                    assert!(received.iter().enumerate().all(|(i, b)| *b == i as u8));
                    ctx.dispatch(received.len() as u32);
                });
                let data: Vec<u8> = (0..LEN).map(|i| i as u8).collect();
                let mut sent = 0;
                while sent < LEN {
                    sent += write.write(ctx, &data[sent..]).await.unwrap();
                }
                drop(write);
                reader.await;
                pending::<()>().await;
            },
        )
        .unwrap();
        run_until_events(&mut event_loop, 1);
        assert_eq!(event_loop.state_mut().0, vec![LEN as u32]);
        server.join().unwrap();
    }
}
