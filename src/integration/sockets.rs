use rustix::{
    fd::OwnedFd,
    net::{AddressFamily, SocketType},
};

use crate::testing::{FdTestingExt, plain_socketpair};

pub(super) fn socketpair() -> (OwnedFd, OwnedFd) {
    let (a, b) = plain_socketpair();
    rustix::io::ioctl_fionbio(&a, true).unwrap();
    rustix::io::ioctl_fionbio(&b, true).unwrap();
    (a, b)
}

pub(super) fn raw_socket(family: AddressFamily, ty: SocketType) -> OwnedFd {
    let fd = rustix::net::socket(family, ty, None).unwrap();
    rustix::io::ioctl_fionbio(&fd, true).unwrap();
    fd
}

pub(super) fn raw_accept(listener: &OwnedFd) -> OwnedFd {
    assert!(listener.wait_readable_blocking(2000), "nobody connected");
    let fd = rustix::net::accept(listener).unwrap();
    rustix::io::ioctl_fionbio(&fd, true).unwrap();
    fd
}
