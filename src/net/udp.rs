use core::net::SocketAddr;

use rustix::fd::OwnedFd;
use rustix::net::SocketType;

use crate::context::Context;
use crate::state::State;

pub struct UdpSocket {
    fd: OwnedFd,
}

impl UdpSocket {
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let fd = super::bind(super::ip_family(&addr), SocketType::DGRAM, &addr)?;
        Ok(Self { fd })
    }

    pub async fn send_to<S: State>(
        &mut self,
        ctx: &Context<S>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> rustix::io::Result<usize> {
        super::send_to(ctx, &self.fd, buf, &addr).await
    }

    pub async fn recv_from<S: State>(
        &mut self,
        ctx: &Context<S>,
        buf: &mut [u8],
    ) -> rustix::io::Result<(usize, SocketAddr)> {
        super::recv_from(ctx, &self.fd, buf).await
    }

    pub fn connect(&self, addr: SocketAddr) -> rustix::io::Result<()> {
        rustix::net::connect(&self.fd, &addr)
    }

    pub async fn send<S: State>(
        &mut self,
        ctx: &Context<S>,
        buf: &[u8],
    ) -> rustix::io::Result<usize> {
        super::send(ctx, &self.fd, buf).await
    }

    pub async fn recv<S: State>(
        &mut self,
        ctx: &Context<S>,
        buf: &mut [u8],
    ) -> rustix::io::Result<usize> {
        super::recv(ctx, &self.fd, buf).await
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        super::local_addr(&self.fd)
    }
}

#[cfg(test)]
mod tests {
    use core::future::pending;
    use std::thread;

    use super::*;
    use crate::EventLoop;
    use crate::test_utils::{Log, TEST_MAX_IO, TEST_MAX_TASKS, TEST_MAX_TIMERS, run_until_events};

    fn std_echo_peer(datagrams: usize) -> (SocketAddr, thread::JoinHandle<()>) {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let peer = thread::spawn(move || {
            let mut buf = [0; 64];
            for _ in 0..datagrams {
                let (len, from) = socket.recv_from(&mut buf).unwrap();
                socket.send_to(&buf[..len], from).unwrap();
            }
        });
        (addr, peer)
    }

    #[test]
    fn round_trip() {
        let (peer_addr, peer) = std_echo_peer(2);
        let mut event_loop = EventLoop::new(
            Log::default(),
            TEST_MAX_TASKS,
            TEST_MAX_TIMERS,
            TEST_MAX_IO,
            async move |ctx| {
                let mut socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
                assert!(socket.local_addr().unwrap().port() != 0);
                let mut buf = [0; 64];

                assert_eq!(socket.send_to(ctx, b"ping", peer_addr).await.unwrap(), 4);
                let (len, from) = socket.recv_from(ctx, &mut buf).await.unwrap();
                assert_eq!((&buf[..len], from), (&b"ping"[..], peer_addr));

                socket.connect(peer_addr).unwrap();
                assert_eq!(socket.send(ctx, b"hello").await.unwrap(), 5);
                let len = socket.recv(ctx, &mut buf).await.unwrap();
                assert_eq!(&buf[..len], b"hello");

                ctx.dispatch(1);
                pending::<()>().await;
            },
        )
        .unwrap();
        run_until_events(&mut event_loop, 1);
        peer.join().unwrap();
    }
}
