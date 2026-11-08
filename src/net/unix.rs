use rustix::{
    fd::OwnedFd,
    net::{AddressFamily, Shutdown, SocketAddrUnix, SocketType},
};

use crate::{
    context::Context,
    net::{ReadHalf, WriteHalf},
    state::State,
};

pub struct UnixListener {
    fd: OwnedFd,
}

impl UnixListener {
    pub fn bind(addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let fd = crate::net::bind(AddressFamily::UNIX, SocketType::STREAM, addr)?;
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
    ) -> rustix::io::Result<(UnixStream, SocketAddrUnix)> {
        let (fd, addr) = crate::net::accept(ctx, &self.fd).await?;
        Ok((UnixStream { fd }, addr))
    }

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        crate::net::local_addr(&self.fd)
    }
}

pub struct UnixStream {
    fd: OwnedFd,
}

impl UnixStream {
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
        addr: &SocketAddrUnix,
    ) -> rustix::io::Result<Self> {
        let fd = crate::net::connect(ctx, AddressFamily::UNIX, addr).await?;
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

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        crate::net::local_addr(&self.fd)
    }

    pub fn peer_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        crate::net::peer_addr(&self.fd)
    }

    pub fn into_split(self) -> rustix::io::Result<(ReadHalf, WriteHalf)> {
        crate::net::split(self.fd)
    }
}

pub struct UnixDatagram {
    fd: OwnedFd,
}

impl UnixDatagram {
    pub fn bind(addr: &SocketAddrUnix) -> rustix::io::Result<Self> {
        let fd = crate::net::bind(AddressFamily::UNIX, SocketType::DGRAM, addr)?;
        Ok(Self { fd })
    }

    pub fn unbound() -> rustix::io::Result<Self> {
        let fd = crate::net::socket(AddressFamily::UNIX, SocketType::DGRAM)?;
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
        addr: &SocketAddrUnix,
    ) -> rustix::io::Result<usize> {
        crate::net::send_to(ctx, &self.fd, buf, addr).await
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
    ) -> rustix::io::Result<(usize, SocketAddrUnix)> {
        crate::net::recv_from(ctx, &self.fd, buf).await
    }

    pub fn connect(&self, addr: &SocketAddrUnix) -> rustix::io::Result<()> {
        rustix::net::connect(&self.fd, addr)
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

    pub fn local_addr(&self) -> rustix::io::Result<SocketAddrUnix> {
        crate::net::local_addr(&self.fd)
    }
}

#[cfg(test)]
mod tests {
    use core::{ffi::CStr, future::pending, mem::MaybeUninit};

    use rustix::net::{AddressFamily, RecvFlags, SendFlags, Shutdown, SocketAddrUnix, SocketType};

    use crate::{
        net::{UnixDatagram, UnixListener, UnixStream},
        test_utils::{
            Log, TEST_MEMORY, TestLoop, TestQueue, drain_until_readable, peer_read_exact,
            raw_accept, raw_socket, run_until_events,
        },
    };

    struct TempPath {
        bytes: [u8; 104],
    }

    impl TempPath {
        fn new(name: &str) -> Self {
            let mut bytes = [0; 104];
            let mut len = 0;
            let mut push = |part: &[u8]| {
                bytes[len..len + part.len()].copy_from_slice(part);
                len += part.len();
            };
            push(b"/tmp/edar-");
            let pid = rustix::process::getpid()
                .as_raw_nonzero()
                .get()
                .unsigned_abs();
            let digits = pid.checked_ilog10().unwrap_or(0) + 1;
            for i in (0..digits).rev() {
                push(&[b'0' + (pid / 10u32.pow(i) % 10) as u8]);
            }
            push(b"-");
            push(name.as_bytes());
            let path = Self { bytes };
            path.unlink();
            path
        }

        fn as_c_str(&self) -> &CStr {
            CStr::from_bytes_until_nul(&self.bytes).unwrap()
        }

        fn addr(&self) -> SocketAddrUnix {
            SocketAddrUnix::new(self.as_c_str()).unwrap()
        }

        fn unlink(&self) {
            let _ = rustix::fs::unlink(self.as_c_str());
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            self.unlink();
        }
    }

    fn path_of(addr: &SocketAddrUnix) -> &[u8] {
        addr.path_bytes().unwrap_or_default()
    }

    #[test]
    fn stream_echo_with_a_raw_peer() {
        let path = TempPath::new("stream-echo");
        let server_addr = path.addr();
        let listener = raw_socket(AddressFamily::UNIX, SocketType::STREAM);
        rustix::net::bind(&listener, &server_addr).unwrap();
        rustix::net::listen(&listener, 1).unwrap();
        let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
        let mut event_loop = TestLoop::new(
            {
                static QUEUE: TestQueue = TestQueue::new();
                &QUEUE
            },
            &mut memory,
            Log::default(),
            async move |ctx| {
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
        let path = TempPath::new("listener");
        let listener_addr = path.addr();
        let mut listener = UnixListener::bind(&listener_addr).unwrap();
        assert_eq!(
            path_of(&listener.local_addr().unwrap()),
            path_of(&listener_addr)
        );
        let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
        let mut event_loop = TestLoop::new(
            {
                static QUEUE: TestQueue = TestQueue::new();
                &QUEUE
            },
            &mut memory,
            Log::default(),
            async move |ctx| {
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
            },
        )
        .unwrap();
        let client = raw_socket(AddressFamily::UNIX, SocketType::STREAM);
        rustix::net::connect(&client, &path.addr()).unwrap();
        assert_eq!(rustix::io::write(&client, b"ping").unwrap(), 4);
        run_until_events(&mut event_loop, 1);
        let mut buf = [0; 4];
        peer_read_exact(&mut event_loop, &client, &mut buf);
        assert_eq!(&buf, b"pong");
    }

    #[test]
    fn datagram_round_trip_with_a_raw_peer() {
        let ours = TempPath::new("dgram-ours");
        let theirs = TempPath::new("dgram-theirs");
        let (ours_addr, theirs_addr) = (ours.addr(), theirs.addr());
        let peer = raw_socket(AddressFamily::UNIX, SocketType::DGRAM);
        rustix::net::bind(&peer, &theirs_addr).unwrap();
        let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
        let mut event_loop = TestLoop::new(
            {
                static QUEUE: TestQueue = TestQueue::new();
                &QUEUE
            },
            &mut memory,
            Log::default(),
            async move |ctx| {
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
            },
        )
        .unwrap();
        let mut buf = [0; 64];
        for expected in [&b"ping"[..], b"hello"] {
            drain_until_readable(&mut event_loop, &peer);
            let (len, _, from) =
                rustix::net::recvfrom(&peer, &mut buf, RecvFlags::empty()).unwrap();
            assert_eq!(&buf[..len], expected);
            let from = SocketAddrUnix::try_from(from.unwrap()).unwrap();
            assert_eq!(path_of(&from), path_of(&ours.addr()));
            let sent = rustix::net::sendto(&peer, &buf[..len], SendFlags::empty(), &from).unwrap();
            assert_eq!(sent, len);
        }
        run_until_events(&mut event_loop, 1);
    }

    #[test]
    fn unbound_datagram_sender_has_an_unnamed_address() {
        let path = TempPath::new("dgram-unbound");
        let bound_addr = path.addr();
        let mut memory = [MaybeUninit::uninit(); TEST_MEMORY];
        let mut event_loop = TestLoop::new(
            {
                static QUEUE: TestQueue = TestQueue::new();
                &QUEUE
            },
            &mut memory,
            Log::default(),
            async move |ctx| {
                let mut bound = UnixDatagram::bind(&bound_addr).unwrap();
                let mut unbound = UnixDatagram::unbound().unwrap();
                unbound.send_to(ctx, b"hi", &bound_addr).await.unwrap();
                let mut buf = [0; 8];
                let (len, from) = bound.recv_from(ctx, &mut buf).await.unwrap();
                assert_eq!(&buf[..len], b"hi");
                assert!(path_of(&from).is_empty());
                ctx.dispatch(1);
                pending::<()>().await;
            },
        )
        .unwrap();
        run_until_events(&mut event_loop, 1);
    }
}
