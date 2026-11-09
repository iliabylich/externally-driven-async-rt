use rustix::{
    fd::OwnedFd,
    net::{AddressFamily, SocketFlags, SocketType},
};

use crate::testing::is_readable;

pub(crate) fn socketpair() -> (OwnedFd, OwnedFd) {
    let (a, b) = rustix::net::socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::empty(),
        None,
    )
    .unwrap();
    rustix::io::ioctl_fionbio(&a, true).unwrap();
    rustix::io::ioctl_fionbio(&b, true).unwrap();
    (a, b)
}

pub(crate) fn raw_socket(family: AddressFamily, ty: SocketType) -> OwnedFd {
    let fd = rustix::net::socket(family, ty, None).unwrap();
    rustix::io::ioctl_fionbio(&fd, true).unwrap();
    fd
}

pub(crate) fn raw_accept(listener: &OwnedFd) -> OwnedFd {
    assert!(is_readable(listener, 2000), "nobody connected");
    let fd = rustix::net::accept(listener).unwrap();
    rustix::io::ioctl_fionbio(&fd, true).unwrap();
    fd
}
