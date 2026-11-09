use core::{
    future::pending,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use rustix::{
    fd::OwnedFd,
    io::Errno,
    net::{AddressFamily, RecvFlags, SendFlags, SocketType},
};

use crate::{
    Selector,
    net::UdpSocket,
    testing::{Log, TestLoop, TestStorage, drain_until_readable, raw_socket, run_until_events},
};

const LOCALHOST: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

fn raw_peer() -> (OwnedFd, SocketAddr) {
    let fd = raw_socket(AddressFamily::INET, SocketType::DGRAM);
    rustix::net::bind(&fd, &LOCALHOST).unwrap();
    let addr = rustix::net::getsockname(&fd).unwrap().try_into().unwrap();
    (fd, addr)
}

#[test]
#[cfg_attr(miri, ignore)]
fn round_trip_with_a_raw_peer() {
    let (peer, peer_addr) = raw_peer();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
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
        let (len, _, from) = rustix::net::recvfrom(&peer, &mut buf, RecvFlags::empty()).unwrap();
        assert_eq!(&buf[..len], expected);
        let from = from.unwrap();
        assert_eq!(
            rustix::net::sendto(&peer, &buf[..len], SendFlags::empty(), &from).unwrap(),
            len
        );
    }
    run_until_events(&mut event_loop, 1);
}

#[test]
#[cfg_attr(miri, ignore)]
fn connected_socket_reports_a_refused_peer() {
    let closed = raw_peer().1;
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        Log::default(),
        async move |ctx| {
            let mut buf = [0; 8];
            let mut socket = UdpSocket::bind(LOCALHOST).unwrap();
            socket.connect(closed).unwrap();
            socket.send(ctx, b"x").await.unwrap();
            let errno = socket.recv(ctx, &mut buf).await.err().unwrap();
            ctx.dispatch(errno.raw_os_error() as u32);

            let mut socket = UdpSocket::bind(LOCALHOST).unwrap();
            socket.connect(closed).unwrap();
            socket.send(ctx, b"x").await.unwrap();
            let errno = socket.recv_from(ctx, &mut buf).await.err().unwrap();
            ctx.dispatch(errno.raw_os_error() as u32);
            pending::<()>().await;
        },
    )
    .unwrap();
    run_until_events(&mut event_loop, 2);
    let refused = Errno::CONNREFUSED.raw_os_error() as u32;
    assert_eq!(event_loop.state_mut().events(), [refused, refused]);
}
