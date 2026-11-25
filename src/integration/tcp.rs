use core::{
    cell::Cell,
    future::{pending, poll_fn},
    net::SocketAddr,
    pin::pin,
    task::Poll,
    time::Duration,
};

use rustix::{
    fd::OwnedFd,
    io::Errno,
    net::{AddressFamily, Shutdown, SocketType},
};

use crate::{
    EventLoop, LoopStorage, NotEnoughResources, Selector,
    io::{AsyncRead, AsyncWrite},
    net::{NetError, TcpListener, TcpStream},
    testing::{EventLoopTestingExt, assert_yields},
};

use super::{EventLoopExt, FdExt, raw_accept, raw_socket};

type Storage<E = u32> = LoopStorage<E, 1, 1, 1024, 2048, 1, 3>;

const LOCALHOST: SocketAddr =
    SocketAddr::new(core::net::IpAddr::V4(core::net::Ipv4Addr::LOCALHOST), 0);

fn raw_listener() -> (OwnedFd, SocketAddr) {
    let fd = raw_socket(AddressFamily::INET, SocketType::STREAM);
    rustix::net::bind(&fd, &LOCALHOST).unwrap();
    rustix::net::listen(&fd, 1).unwrap();
    let addr = rustix::net::getsockname(&fd).unwrap().try_into().unwrap();
    (fd, addr)
}

#[test]
fn echo_with_a_raw_peer() {
    static STORAGE: Storage = Storage::new();
    let (listener, addr) = raw_listener();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        let mut stream = TcpStream::connect(&ctx, addr).await.unwrap();
        assert_eq!(stream.peer_addr().unwrap(), addr);
        assert!(stream.local_addr().unwrap().ip().is_loopback());
        assert_eq!(stream.write(b"hello").await.unwrap(), 5);
        let mut buf = [0; 5];
        let mut len = 0;
        while len < 5 {
            len += stream.read(&mut buf[len..]).await.unwrap();
        }
        assert_eq!(&buf, b"hello");
        stream.shutdown(Shutdown::Write).unwrap();
        assert_eq!(stream.read(&mut buf).await.unwrap(), 0);
        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    let peer = raw_accept(&listener);
    let mut buf = [0; 5];
    event_loop.peer_read_exact(&peer, &mut buf);
    assert_eq!(&buf, b"hello");
    assert_eq!(rustix::io::write(&peer, &buf).unwrap(), 5);
    event_loop.drain_until_readable(&peer);
    assert_eq!(rustix::io::read(&peer, &mut buf).unwrap(), 0, "no EOF");
    drop(peer);
    event_loop.run_until_events::<1>();
}

#[test]
fn listener_accepts_a_raw_client() {
    static STORAGE: Storage = Storage::new();
    let listening = Cell::new(None);
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let mut listener = TcpListener::bind(&ctx, LOCALHOST).unwrap();
        listening.set(Some(listener.local_addr().unwrap()));
        let (mut stream, peer) = listener.accept().await.unwrap();
        assert_eq!(stream.peer_addr().unwrap(), peer);
        let mut buf = [0; 4];
        let mut len = 0;
        while len < 4 {
            len += stream.read(&mut buf[len..]).await.unwrap();
        }
        assert_eq!(&buf, b"ping");
        stream.write(b"pong").await.unwrap();
        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    let addr = listening.take().unwrap();
    let client = raw_socket(AddressFamily::INET, SocketType::STREAM);
    match rustix::net::connect(&client, &addr) {
        Ok(()) | Err(Errno::INPROGRESS) => {}
        Err(errno) => panic!("{errno}"),
    }
    assert!(client.wait_writable_blocking(2000), "never connected");
    assert_eq!(rustix::io::write(&client, b"ping").unwrap(), 4);
    event_loop.run_until_events::<1>();
    let mut buf = [0; 4];
    event_loop.peer_read_exact(&client, &mut buf);
    assert_eq!(&buf, b"pong");
}

#[test]
fn connect_to_a_closed_port_fails() {
    static STORAGE: Storage<NetError> = Storage::new();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let addr = TcpListener::bind(&ctx, LOCALHOST)
            .unwrap()
            .local_addr()
            .unwrap();
        let err = TcpStream::connect(&ctx, addr).await.err().unwrap();
        ctx.dispatch(err).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_eq!(
        event_loop.run_until_events(),
        [NetError::Os(Errno::CONNREFUSED)]
    );
}

#[test]
fn borrowed_halves_read_and_write_concurrently_in_one_task() {
    static STORAGE: Storage = Storage::new();
    const LEN: usize = 1 << 20;
    const CHUNK: usize = 512;
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let mut listener = TcpListener::bind(&ctx, LOCALHOST).unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = ctx
            .spawn(async move |_| {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buf = [0; CHUNK];
                loop {
                    let len = stream.read(&mut buf).await.unwrap();
                    if len == 0 {
                        return;
                    }
                    let mut sent = 0;
                    while sent < len {
                        sent += stream.write(&buf[sent..len]).await.unwrap();
                    }
                }
            })
            .unwrap();
        drop(handle);
        let mut stream = TcpStream::connect(&ctx, addr).await.unwrap();
        let (mut read, mut write) = stream.split();
        let mut reading = pin!(async {
            let mut buf = [0; CHUNK];
            let mut received = 0;
            while received < LEN {
                let len = read.read(&mut buf).await.unwrap();
                assert_ne!(len, 0);
                for (i, byte) in buf[..len].iter().enumerate() {
                    assert_eq!(*byte, u8::try_from((received + i) % 256).unwrap());
                }
                received += len;
            }
            received
        });
        let mut writing = pin!(async {
            let mut sent = 0;
            while sent < LEN {
                let chunk: [u8; CHUNK] =
                    core::array::from_fn(|i| u8::try_from((sent + i) % 256).unwrap());
                let len = CHUNK.min(LEN - sent);
                sent += write.write(&chunk[..len]).await.unwrap();
            }
        });
        let (mut received, mut written) = (None, false);
        poll_fn(|cx| {
            if received.is_none()
                && let Poll::Ready(len) = reading.as_mut().poll(cx)
            {
                received = Some(len);
            }
            if !written && writing.as_mut().poll(cx).is_ready() {
                written = true;
            }
            if received.is_some() && written {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        ctx.dispatch(u32::try_from(received.unwrap()).unwrap())
            .unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_eq!(event_loop.run_until_events(), [u32::try_from(LEN).unwrap()]);
}

#[test]
fn listener_binds_to_ipv6() {
    static STORAGE: Storage = Storage::new();
    let localhost = SocketAddr::new(core::net::IpAddr::V6(core::net::Ipv6Addr::LOCALHOST), 0);
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let listener = TcpListener::bind(&ctx, localhost).unwrap();
        assert!(listener.local_addr().unwrap().is_ipv6());
        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), [1]);
}

#[test]
fn one_fd_slot_holds_one_socket_at_a_time() {
    static STORAGE: LoopStorage<u32, 1, 1, 0, 1024, 1, 1> = LoopStorage::new();
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        for _ in 0..3 {
            drop(TcpListener::bind(&ctx, LOCALHOST).unwrap());
        }
        let _first = TcpListener::bind(&ctx, LOCALHOST).unwrap();
        assert_eq!(
            TcpListener::bind(&ctx, LOCALHOST).err(),
            Some(NetError::NotEnoughResources(NotEnoughResources::FDs))
        );
        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(event_loop.events(), [1]);
}

#[test]
fn split_halves_wait_for_readiness_and_try_io() {
    static STORAGE: Storage = Storage::new();
    let (listener, addr) = raw_listener();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        let mut stream = TcpStream::connect(&ctx, addr).await.unwrap();
        let (mut rx, mut tx) = stream.split();
        let mut buf = [0; 5];
        assert_eq!(rx.try_read(&mut buf), Err(Errno::AGAIN));

        tx.writable().await;
        assert_eq!(tx.try_write(b"hello").unwrap(), 5);

        rx.readable().await;
        assert_eq!(rx.try_read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"world");
        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    let peer = raw_accept(&listener);
    let mut buf = [0; 5];
    event_loop.peer_read_exact(&peer, &mut buf);
    assert_eq!(&buf, b"hello");
    assert_eq!(rustix::io::write(&peer, b"world").unwrap(), 5);
    event_loop.run_until_events::<1>();
}

#[test]
fn set_keepalive_on_a_connected_stream() {
    static STORAGE: Storage = Storage::new();
    let (listener, addr) = raw_listener();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        let stream = TcpStream::connect(&ctx, addr).await.unwrap();
        stream
            .set_keepalive(Duration::from_secs(1), Duration::from_secs(1), 2)
            .unwrap();
        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    let _peer = raw_accept(&listener);
    event_loop.run_until_events::<1>();
}
