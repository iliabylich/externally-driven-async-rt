use core::net::SocketAddr;

use rustix::{fd::OwnedFd, net::SocketType};

use crate::{context::Context, state::State};

pub struct UdpSocket {
    fd: OwnedFd,
}

impl UdpSocket {
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let fd = crate::net::bind(crate::net::ip_family(&addr), SocketType::DGRAM, &addr)?;
        Ok(Self { fd })
    }

    pub async fn send_to<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &mut self,
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
        addr: SocketAddr,
    ) -> rustix::io::Result<usize> {
        crate::net::send_to(ctx, &self.fd, buf, &addr).await
    }

    pub async fn recv_from<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &mut self,
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
    ) -> rustix::io::Result<(usize, SocketAddr)> {
        crate::net::recv_from(ctx, &self.fd, buf).await
    }

    pub fn connect(&self, addr: SocketAddr) -> rustix::io::Result<()> {
        rustix::net::connect(&self.fd, &addr)
    }

    pub async fn send<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &mut self,
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
        crate::net::send(ctx, &self.fd, buf).await
    }

    pub async fn recv<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
    >(
        &mut self,
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
        crate::net::recv(ctx, &self.fd, buf).await
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        crate::net::local_addr(&self.fd)
    }
}

#[cfg(test)]
mod tests {
    use core::{
        future::pending,
        mem::MaybeUninit,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use rustix::{
        fd::OwnedFd,
        net::{AddressFamily, RecvFlags, SendFlags, SocketType},
    };

    use crate::{
        net::UdpSocket,
        test_utils::{
            Log, TEST_MEMORY, TEST_QUEUE_MEMORY, TestLoop, drain_until_readable, raw_socket,
            run_until_events,
        },
    };

    const LOCALHOST: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    fn raw_peer() -> (OwnedFd, SocketAddr) {
        let fd = raw_socket(AddressFamily::INET, SocketType::DGRAM);
        rustix::net::bind(&fd, &LOCALHOST).unwrap();
        let addr = rustix::net::getsockname(&fd).unwrap().try_into().unwrap();
        (fd, addr)
    }

    #[test]
    fn round_trip_with_a_raw_peer() {
        let (peer, peer_addr) = raw_peer();
        let mut event_loop = TestLoop::new(
            {
                static mut QUEUE: [MaybeUninit<u8>; TEST_QUEUE_MEMORY] =
                    [MaybeUninit::uninit(); TEST_QUEUE_MEMORY];
                // SAFETY: this block runs once, on this test's thread, so this is the only
                // reference to `QUEUE` ever created.
                unsafe { (&raw mut QUEUE).as_mut_unchecked() }
            },
            {
                static mut MEMORY: [MaybeUninit<u8>; TEST_MEMORY] =
                    [MaybeUninit::uninit(); TEST_MEMORY];
                // SAFETY: this block runs once, on this test's thread, so this is the only
                // reference to `MEMORY` ever created.
                unsafe { (&raw mut MEMORY).as_mut_unchecked() }
            },
            Log::default(),
            async move |ctx| {
                let mut socket = UdpSocket::bind(LOCALHOST).unwrap();
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
        let mut buf = [0; 64];
        for expected in [&b"ping"[..], b"hello"] {
            drain_until_readable(&mut event_loop, &peer);
            let (len, _, from) =
                rustix::net::recvfrom(&peer, &mut buf, RecvFlags::empty()).unwrap();
            assert_eq!(&buf[..len], expected);
            let from = from.unwrap();
            assert_eq!(
                rustix::net::sendto(&peer, &buf[..len], SendFlags::empty(), &from).unwrap(),
                len
            );
        }
        run_until_events(&mut event_loop, 1);
    }
}
