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
    Either2, EventLoop, LoopStorage, Selector,
    net::{NetError, UdpSocket},
    select2,
    testing::{EventLoopTestingExt, TestLoop, TestStorage, assert_yields, raw_socket},
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
        async move |ctx| {
            let mut socket = UdpSocket::bind(LOCALHOST).unwrap();
            assert!(socket.local_addr().unwrap().port() != 0);
            let mut buf = [0; 64];

            assert_eq!(socket.send_to(&ctx, b"ping", peer_addr).await.unwrap(), 4);
            let (len, from) = socket.recv_from(&ctx, &mut buf).await.unwrap();
            assert_eq!((&buf[..len], from), (&b"ping"[..], peer_addr));

            socket.connect(peer_addr).unwrap();
            assert_eq!(socket.send(&ctx, b"hello").await.unwrap(), 5);
            let len = socket.recv(&ctx, &mut buf).await.unwrap();
            assert_eq!(&buf[..len], b"hello");

            ctx.dispatch(1).unwrap();
            pending::<()>().await;
        },
    )
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
#[cfg_attr(miri, ignore)]
fn connected_socket_reports_a_refused_peer() {
    let closed = raw_peer().1;
    let mut selector = Selector::new().unwrap();
    let mut event_loop = TestLoop::new(
        {
            static STORAGE: TestStorage<NetError> = TestStorage::new();
            &STORAGE
        },
        &mut selector,
        async move |ctx| {
            let mut buf = [0; 8];
            let mut socket = UdpSocket::bind(LOCALHOST).unwrap();
            socket.connect(closed).unwrap();
            socket.send(&ctx, b"x").await.unwrap();
            let err = socket.recv(&ctx, &mut buf).await.err().unwrap();
            ctx.dispatch(err).unwrap();

            let mut socket = UdpSocket::bind(LOCALHOST).unwrap();
            socket.connect(closed).unwrap();
            socket.send(&ctx, b"x").await.unwrap();
            let err = socket.recv_from(&ctx, &mut buf).await.err().unwrap();
            ctx.dispatch(err).unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();
    let refused = NetError::Os(Errno::CONNREFUSED);
    assert_eq!(event_loop.run_until_events(), [refused, refused]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn waiting_on_more_sockets_than_fds_fails() {
    let mut selector = Selector::new().unwrap();
    let mut event_loop = EventLoop::new(
        {
            static STORAGE: LoopStorage<NetError, 1, 1, 64, 1024, 1, 1> = LoopStorage::new();
            &STORAGE
        },
        &mut selector,
        async |ctx| {
            let mut first = UdpSocket::bind(LOCALHOST).unwrap();
            let mut second = UdpSocket::bind(LOCALHOST).unwrap();
            let (mut first_buf, mut second_buf) = ([0; 1], [0; 1]);
            let Either2::Right(result) = select2(
                first.recv(&ctx, &mut first_buf),
                second.recv(&ctx, &mut second_buf),
            )
            .await
            else {
                panic!("nothing is sent to the first socket");
            };
            ctx.dispatch(result.unwrap_err()).unwrap();
            pending::<()>().await;
        },
    )
    .unwrap();
    assert_yields!(event_loop.events(), [NetError::FdsFull]);
}
