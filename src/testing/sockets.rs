use rustix::{
    fd::OwnedFd,
    net::{AddressFamily, SocketFlags, SocketType},
};

pub(crate) fn plain_socketpair() -> (OwnedFd, OwnedFd) {
    rustix::net::socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::empty(),
        None,
    )
    .unwrap()
}
