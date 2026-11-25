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
    EventLoop, LoopStorage, NotEnoughResources, Selector,
    net::{NetError, UdpSocket},
    testing::{EventLoopTestingExt, assert_yields},
};

use super::{EventLoopExt, raw_socket};

type Storage<E> = LoopStorage<E, 2, 1, 0, 1024, 1, 1>;

const LOCALHOST: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

fn raw_peer() -> (OwnedFd, SocketAddr) {
    let fd = raw_socket(AddressFamily::INET, SocketType::DGRAM);
    rustix::net::bind(&fd, &LOCALHOST).unwrap();
    let addr = rustix::net::getsockname(&fd).unwrap().try_into().unwrap();
    (fd, addr)
}

#[test]
fn round_trip_with_a_raw_peer() {
    static STORAGE: Storage<u32> = Storage::new();
    let (peer, peer_addr) = raw_peer();
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        let mut socket = UdpSocket::bind(&ctx, LOCALHOST).unwrap();
        assert!(socket.local_addr().unwrap().port() != 0);
        let mut buf = [0; 64];

        assert_eq!(socket.send_to(b"ping", peer_addr).await.unwrap(), 4);
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        assert_eq!((&buf[..len], from), (&b"ping"[..], peer_addr));

        socket.connect(peer_addr).unwrap();
        assert_eq!(socket.send(b"hello").await.unwrap(), 5);
        let len = socket.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], b"hello");

        ctx.dispatch(1).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    let mut buf = [0; 64];
    for expected in [&b"ping"[..], b"hello"] {
        event_loop.drain_until_readable(&peer);
        let (len, _, from) = rustix::net::recvfrom(&peer, &mut buf, RecvFlags::empty()).unwrap();
        assert_eq!(&buf[..len], expected);
        let from = from.unwrap();
        assert_eq!(
            rustix::net::sendto(&peer, &buf[..len], SendFlags::empty(), &from).unwrap(),
            len
        );
    }
    event_loop.run_until_events::<1>();
}

#[test]
fn connected_socket_reports_a_refused_peer() {
    static STORAGE: Storage<NetError> = Storage::new();
    let closed = raw_peer().1;
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(&STORAGE, &mut selector, async move |ctx| {
        let mut buf = [0; 8];
        let mut socket = UdpSocket::bind(&ctx, LOCALHOST).unwrap();
        socket.connect(closed).unwrap();
        socket.send(b"x").await.unwrap();
        let err = socket.recv(&mut buf).await.err().unwrap();
        ctx.dispatch(err).unwrap();
        drop(socket);

        let mut socket = UdpSocket::bind(&ctx, LOCALHOST).unwrap();
        socket.connect(closed).unwrap();
        socket.send(b"x").await.unwrap();
        let err = socket.recv_from(&mut buf).await.err().unwrap();
        ctx.dispatch(err).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    let refused = NetError::Os(Errno::CONNREFUSED);
    assert_eq!(event_loop.run_until_events(), [refused, refused]);
}

#[test]
fn binding_more_sockets_than_fds_fails() {
    static STORAGE: Storage<NetError> = Storage::new();
    let mut selector = Selector::new().unwrap();
    let event_loop = EventLoop::new(&STORAGE, &mut selector, async |ctx| {
        let _first = UdpSocket::bind(&ctx, LOCALHOST).unwrap();
        let err = UdpSocket::bind(&ctx, LOCALHOST).err().unwrap();
        ctx.dispatch(err).unwrap();
        pending::<()>().await;
    })
    .unwrap();
    assert_yields!(
        event_loop.events(),
        [NetError::NotEnoughResources(NotEnoughResources::FDs)]
    );
}
