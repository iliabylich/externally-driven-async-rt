#![cfg(not(miri))]

mod event_loop;
mod event_loop_ext;
mod fd_ext;
mod io;
mod notification;
mod selector;
mod sockets;
mod tcp;
mod udp;
mod unix;

use self::{
    event_loop_ext::EventLoopExt,
    fd_ext::FdExt,
    sockets::{raw_accept, raw_socket, socketpair},
};
