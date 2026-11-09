use core::{future::pending, net::SocketAddr};

use rustix::{
    fd::OwnedFd,
    io::Errno,
    net::{AddressFamily, Shutdown, SocketType},
};

use crate::{
    Selector,
    net::{TcpListener, TcpStream},
    testing::{
        self, Log, TestLoop, TestStorage, drain_until_readable, is_writable, peer_read_exact,
        raw_accept, run_until_events,
    },
};

const LOCALHOST: SocketAddr =
    SocketAddr::new(core::net::IpAddr::V4(core::net::Ipv4Addr::LOCALHOST), 0);

fn raw_socket() -> OwnedFd {
    testing::raw_socket(AddressFamily::INET, SocketType::STREAM)
}

fn raw_listener() -> (OwnedFd, SocketAddr) {
    let fd = raw_socket();
    rustix::net::bind(&fd, &LOCALHOST).unwrap();
    rustix::net::listen(&fd, 1).unwrap();
    let addr = rustix::net::getsockname(&fd).unwrap().try_into().unwrap();
    (fd, addr)
}

#[test]
#[cfg_attr(miri, ignore)]
fn echo_with_a_raw_peer() {
    let (listener, addr) = raw_listener();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
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
#[cfg_attr(miri, ignore)]
fn listener_accepts_a_raw_client() {
    let mut listener = TcpListener::bind(LOCALHOST).unwrap();
    let addr = listener.local_addr().unwrap();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
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
#[cfg_attr(miri, ignore)]
fn connect_to_a_closed_port_fails() {
    let addr = TcpListener::bind(LOCALHOST).unwrap().local_addr().unwrap();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
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
#[cfg_attr(miri, ignore)]
fn halves_are_used_from_two_tasks() {
    const LEN: usize = 1 << 20;
    const CHUNK: usize = 512;
    let mut listener = TcpListener::bind(LOCALHOST).unwrap();
    let addr = listener.local_addr().unwrap();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
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

#[test]
#[cfg_attr(miri, ignore)]
fn listener_binds_to_ipv6() {
    let localhost = SocketAddr::new(core::net::IpAddr::V6(core::net::Ipv6Addr::LOCALHOST), 0);
    let listener = TcpListener::bind(localhost).unwrap();
    assert!(listener.local_addr().unwrap().is_ipv6());
}
