use core::cell::RefCell;
use core::{ffi::CStr, future::pending};

use rustix::{
    io::Errno,
    net::{AddressFamily, RecvFlags, SendFlags, Shutdown, SocketAddrUnix, SocketType},
};

use crate::{
    Selector,
    net::{UnixDatagram, UnixListener, UnixStream},
    testing::{EventLoopTestingExt, Log, TestLoop, TestStorage, raw_accept, raw_socket},
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
#[cfg_attr(miri, ignore)]
fn stream_echo_with_a_raw_peer() {
    let path = TempPath::new("stream-echo");
    let server_addr = path.addr();
    let listener = raw_socket(AddressFamily::UNIX, SocketType::STREAM);
    rustix::net::bind(&listener, &server_addr).unwrap();
    rustix::net::listen(&listener, 1).unwrap();
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let mut stream = UnixStream::connect(&ctx, &server_addr).await.unwrap();
            assert_eq!(path_of(&stream.peer_addr().unwrap()), path_of(&server_addr));
            assert!(path_of(&stream.local_addr().unwrap()).is_empty());
            stream.write(&ctx, b"hello").await.unwrap();
            let mut buf = [0; 5];
            let mut len = 0;
            while len < 5 {
                len += stream.read(&ctx, &mut buf[len..]).await.unwrap();
            }
            assert_eq!(&buf, b"hello");
            stream.shutdown(Shutdown::Write).unwrap();
            assert_eq!(stream.read(&ctx, &mut buf).await.unwrap(), 0);
            ctx.dispatch(1);
            pending::<()>().await;
        },
    )
    .unwrap();
    let peer = raw_accept(&listener);
    let mut buf = [0; 5];
    event_loop.peer_read_exact(&peer, &mut buf);
    assert_eq!(&buf, b"hello");
    assert_eq!(rustix::io::write(&peer, &buf).unwrap(), 5);
    event_loop.drain_until_readable(&peer);
    assert_eq!(rustix::io::read(&peer, &mut buf).unwrap(), 0, "no EOF");
    drop(peer);
    event_loop.run_until_events(1);
}

#[test]
#[cfg_attr(miri, ignore)]
fn listener_accepts_a_raw_client() {
    let path = TempPath::new("listener");
    let listener_addr = path.addr();
    let mut listener = UnixListener::bind(&listener_addr).unwrap();
    assert_eq!(
        path_of(&listener.local_addr().unwrap()),
        path_of(&listener_addr)
    );
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let (mut stream, peer) = listener.accept(&ctx).await.unwrap();
            assert!(path_of(&peer).is_empty());
            let mut buf = [0; 4];
            let mut len = 0;
            while len < 4 {
                len += stream.read(&ctx, &mut buf[len..]).await.unwrap();
            }
            assert_eq!(&buf, b"ping");
            stream.write(&ctx, b"pong").await.unwrap();
            ctx.dispatch(1);
            pending::<()>().await;
        },
    )
    .unwrap();
    let client = raw_socket(AddressFamily::UNIX, SocketType::STREAM);
    rustix::net::connect(&client, &path.addr()).unwrap();
    assert_eq!(rustix::io::write(&client, b"ping").unwrap(), 4);
    event_loop.run_until_events(1);
    let mut buf = [0; 4];
    event_loop.peer_read_exact(&client, &mut buf);
    assert_eq!(&buf, b"pong");
}

#[test]
#[cfg_attr(miri, ignore)]
fn datagram_round_trip_with_a_raw_peer() {
    let ours = TempPath::new("dgram-ours");
    let theirs = TempPath::new("dgram-theirs");
    let (ours_addr, theirs_addr) = (ours.addr(), theirs.addr());
    let peer = raw_socket(AddressFamily::UNIX, SocketType::DGRAM);
    rustix::net::bind(&peer, &theirs_addr).unwrap();
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let mut socket = UnixDatagram::bind(&ours_addr).unwrap();
            assert_eq!(path_of(&socket.local_addr().unwrap()), path_of(&ours_addr));
            let mut buf = [0; 64];

            socket.send_to(&ctx, b"ping", &theirs_addr).await.unwrap();
            let (len, from) = socket.recv_from(&ctx, &mut buf).await.unwrap();
            assert_eq!(&buf[..len], b"ping");
            assert_eq!(path_of(&from), path_of(&theirs_addr));

            socket.connect(&theirs_addr).unwrap();
            socket.send(&ctx, b"hello").await.unwrap();
            let len = socket.recv(&ctx, &mut buf).await.unwrap();
            assert_eq!(&buf[..len], b"hello");

            ctx.dispatch(1);
            pending::<()>().await;
        },
    )
    .unwrap();
    let mut buf = [0; 64];
    for expected in [&b"ping"[..], b"hello"] {
        event_loop.drain_until_readable(&peer);
        let (len, _, from) = rustix::net::recvfrom(&peer, &mut buf, RecvFlags::empty()).unwrap();
        assert_eq!(&buf[..len], expected);
        let from = SocketAddrUnix::try_from(from.unwrap()).unwrap();
        assert_eq!(path_of(&from), path_of(&ours.addr()));
        let sent = rustix::net::sendto(&peer, &buf[..len], SendFlags::empty(), &from).unwrap();
        assert_eq!(sent, len);
    }
    event_loop.run_until_events(1);
}

#[test]
#[cfg_attr(miri, ignore)]
fn unbound_datagram_sender_has_an_unnamed_address() {
    let path = TempPath::new("dgram-unbound");
    let bound_addr = path.addr();
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let mut bound = UnixDatagram::bind(&bound_addr).unwrap();
            let mut unbound = UnixDatagram::unbound().unwrap();
            unbound.send_to(&ctx, b"hi", &bound_addr).await.unwrap();
            let mut buf = [0; 8];
            let (len, from) = bound.recv_from(&ctx, &mut buf).await.unwrap();
            assert_eq!(&buf[..len], b"hi");
            assert!(path_of(&from).is_empty());
            ctx.dispatch(1);
            pending::<()>().await;
        },
    )
    .unwrap();
    event_loop.run_until_events(1);
}

#[test]
#[cfg_attr(miri, ignore)]
fn connect_to_a_missing_path_fails() {
    let path = TempPath::new("missing");
    let addr = path.addr();
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let errno = UnixStream::connect(&ctx, &addr).await.err().unwrap();
            ctx.dispatch(errno.raw_os_error() as u32);
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_eq!(
        event_loop.state_mut().events(),
        [Errno::NOENT.raw_os_error() as u32]
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn stream_halves_echo_with_a_raw_peer() {
    let path = TempPath::new("stream-halves");
    let server_addr = path.addr();
    let listener = raw_socket(AddressFamily::UNIX, SocketType::STREAM);
    rustix::net::bind(&listener, &server_addr).unwrap();
    rustix::net::listen(&listener, 1).unwrap();
    let mut selector = Selector::new().unwrap();
    let mut state = RefCell::new(Log::default());
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        &mut state,
        async move |ctx| {
            let mut stream = UnixStream::connect(&ctx, &server_addr).await.unwrap();
            let (mut read, mut write) = stream.split();
            write.write(&ctx, b"ping").await.unwrap();
            let mut buf = [0; 4];
            let mut len = 0;
            while len < 4 {
                len += read.read(&ctx, &mut buf[len..]).await.unwrap();
            }
            assert_eq!(&buf, b"ping");
            ctx.dispatch(1);
            pending::<()>().await;
        },
    )
    .unwrap();
    let peer = raw_accept(&listener);
    let mut buf = [0; 4];
    event_loop.peer_read_exact(&peer, &mut buf);
    assert_eq!(rustix::io::write(&peer, &buf).unwrap(), 4);
    event_loop.run_until_events(1);
}
