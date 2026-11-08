use core::net::SocketAddr;

use rustix::{
    fd::OwnedFd,
    net::{Shutdown, SocketType, sockopt::set_socket_reuseaddr},
};

use crate::{
    context::Context,
    net::{ReadHalf, WriteHalf},
    state::State,
};

pub struct TcpListener {
    fd: OwnedFd,
}

impl TcpListener {
    pub fn bind(addr: SocketAddr) -> rustix::io::Result<Self> {
        let fd = crate::net::socket(crate::net::ip_family(&addr), SocketType::STREAM)?;
        set_socket_reuseaddr(&fd, true)?;
        rustix::net::bind(&fd, &addr)?;
        crate::net::listen(&fd)?;
        Ok(Self { fd })
    }

    pub async fn accept<
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
    ) -> rustix::io::Result<(TcpStream, SocketAddr)> {
        let (fd, addr) = crate::net::accept(ctx, &self.fd).await?;
        Ok((TcpStream { fd }, addr))
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        crate::net::local_addr(&self.fd)
    }
}

pub struct TcpStream {
    fd: OwnedFd,
}

impl TcpStream {
    pub async fn connect<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
        const MAX_IO_COUNT: usize,
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
        addr: SocketAddr,
    ) -> rustix::io::Result<Self> {
        let fd = crate::net::connect(ctx, crate::net::ip_family(&addr), &addr).await?;
        Ok(Self { fd })
    }

    pub async fn read<
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
        crate::net::read(ctx, &self.fd, buf).await
    }

    pub async fn write<
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
        crate::net::write(ctx, &self.fd, buf).await
    }

    pub fn shutdown(&self, how: Shutdown) -> rustix::io::Result<()> {
        rustix::net::shutdown(&self.fd, how)
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddr> {
        crate::net::local_addr(&self.fd)
    }

    pub fn peer_addr(&self) -> rustix::io::Result<SocketAddr> {
        crate::net::peer_addr(&self.fd)
    }

    pub fn into_split(self) -> rustix::io::Result<(ReadHalf, WriteHalf)> {
        crate::net::split(self.fd)
    }
}

#[cfg(test)]
mod tests {
    use core::{future::pending, mem::MaybeUninit, net::SocketAddr};

    use rustix::{
        fd::OwnedFd,
        io::Errno,
        net::{AddressFamily, Shutdown, SocketType},
    };

    use crate::{
        net::{TcpListener, TcpStream},
        test_utils::{
            self, Log, TEST_MEMORY, TEST_QUEUE_MEMORY, TestLoop, drain_until_readable, is_writable,
            peer_read_exact, raw_accept, run_until_events,
        },
    };

    const LOCALHOST: SocketAddr =
        SocketAddr::new(core::net::IpAddr::V4(core::net::Ipv4Addr::LOCALHOST), 0);

    fn raw_socket() -> OwnedFd {
        test_utils::raw_socket(AddressFamily::INET, SocketType::STREAM)
    }

    fn raw_listener() -> (OwnedFd, SocketAddr) {
        let fd = raw_socket();
        rustix::net::bind(&fd, &LOCALHOST).unwrap();
        rustix::net::listen(&fd, 1).unwrap();
        let addr = rustix::net::getsockname(&fd).unwrap().try_into().unwrap();
        (fd, addr)
    }

    #[test]
    fn echo_with_a_raw_peer() {
        let (listener, addr) = raw_listener();
        let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
        let mut event_loop = TestLoop::new(
            {
                static mut QUEUE: [MaybeUninit<u8>; TEST_QUEUE_MEMORY] =
                    [MaybeUninit::uninit(); TEST_QUEUE_MEMORY];
                // SAFETY: this block runs once, on this test's thread, so this is the only
                // reference to `QUEUE` ever created.
                unsafe { (&raw mut QUEUE).as_mut_unchecked() }
            },
            &mut memory,
            Log::default(),
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
        let peer = raw_accept(&listener);
        let mut buf = [0; 5];
        peer_read_exact(&mut event_loop, &peer, &mut buf);
        assert_eq!(&buf, b"hello");
        assert_eq!(rustix::io::write(&peer, &buf).unwrap(), 5);
        drain_until_readable(&mut event_loop, &peer);
        assert_eq!(rustix::io::read(&peer, &mut buf).unwrap(), 0, "no EOF");
        drop(peer);
        run_until_events(&mut event_loop, 1);
    }

    #[test]
    fn listener_accepts_a_raw_client() {
        let mut listener = TcpListener::bind(LOCALHOST).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
        let mut event_loop = TestLoop::new(
            {
                static mut QUEUE: [MaybeUninit<u8>; TEST_QUEUE_MEMORY] =
                    [MaybeUninit::uninit(); TEST_QUEUE_MEMORY];
                // SAFETY: this block runs once, on this test's thread, so this is the only
                // reference to `QUEUE` ever created.
                unsafe { (&raw mut QUEUE).as_mut_unchecked() }
            },
            &mut memory,
            Log::default(),
            async move |ctx| {
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
        let client = raw_socket();
        match rustix::net::connect(&client, &addr) {
            Ok(()) | Err(Errno::INPROGRESS) => {}
            Err(errno) => panic!("{errno}"),
        }
        assert!(is_writable(&client, 2000), "never connected");
        assert_eq!(rustix::io::write(&client, b"ping").unwrap(), 4);
        run_until_events(&mut event_loop, 1);
        let mut buf = [0; 4];
        peer_read_exact(&mut event_loop, &client, &mut buf);
        assert_eq!(&buf, b"pong");
    }

    #[test]
    fn connect_to_a_closed_port_fails() {
        let addr = TcpListener::bind(LOCALHOST).unwrap().local_addr().unwrap();
        let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
        let mut event_loop = TestLoop::new(
            {
                static mut QUEUE: [MaybeUninit<u8>; TEST_QUEUE_MEMORY] =
                    [MaybeUninit::uninit(); TEST_QUEUE_MEMORY];
                // SAFETY: this block runs once, on this test's thread, so this is the only
                // reference to `QUEUE` ever created.
                unsafe { (&raw mut QUEUE).as_mut_unchecked() }
            },
            &mut memory,
            Log::default(),
            async move |ctx| {
                let errno = TcpStream::connect(ctx, addr).await.err().unwrap();
                ctx.dispatch(errno.raw_os_error() as u32);
                pending::<()>().await;
            },
        )
        .unwrap();
        run_until_events(&mut event_loop, 1);
        assert_eq!(
            event_loop.state_mut().events(),
            [Errno::CONNREFUSED.raw_os_error() as u32]
        );
    }

    #[test]
    fn halves_are_used_from_two_tasks() {
        const LEN: usize = 1 << 20;
        const CHUNK: usize = 512;
        let mut listener = TcpListener::bind(LOCALHOST).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
        let mut event_loop = TestLoop::new(
            {
                static mut QUEUE: [MaybeUninit<u8>; TEST_QUEUE_MEMORY] =
                    [MaybeUninit::uninit(); TEST_QUEUE_MEMORY];
                // SAFETY: this block runs once, on this test's thread, so this is the only
                // reference to `QUEUE` ever created.
                unsafe { (&raw mut QUEUE).as_mut_unchecked() }
            },
            &mut memory,
            Log::default(),
            async move |ctx| {
                ctx.spawn(async move |ctx| {
                    let (mut stream, _) = listener.accept(ctx).await.unwrap();
                    let mut buf = [0; CHUNK];
                    loop {
                        let len = stream.read(ctx, &mut buf).await.unwrap();
                        if len == 0 {
                            return;
                        }
                        let mut sent = 0;
                        while sent < len {
                            sent += stream.write(ctx, &buf[sent..len]).await.unwrap();
                        }
                    }
                });
                let stream = TcpStream::connect(ctx, addr).await.unwrap();
                let (mut read, mut write) = stream.into_split().unwrap();
                let reader = ctx.spawn(async move |ctx| {
                    let mut buf = [0; CHUNK];
                    let mut received = 0;
                    loop {
                        let len = read.read(ctx, &mut buf).await.unwrap();
                        if len == 0 {
                            break;
                        }
                        for (i, byte) in buf[..len].iter().enumerate() {
                            assert_eq!(*byte, (received + i) as u8);
                        }
                        received += len;
                    }
                    ctx.dispatch(received as u32);
                });
                let mut sent = 0;
                while sent < LEN {
                    let chunk: [u8; CHUNK] = core::array::from_fn(|i| (sent + i) as u8);
                    let len = CHUNK.min(LEN - sent);
                    sent += write.write(ctx, &chunk[..len]).await.unwrap();
                }
                drop(write);
                reader.await;
                pending::<()>().await;
            },
        )
        .unwrap();
        run_until_events(&mut event_loop, 1);
        assert_eq!(event_loop.state_mut().events(), [LEN as u32]);
    }
}
