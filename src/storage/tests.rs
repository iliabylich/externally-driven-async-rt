use core::{
    cell::Cell,
    future::Future,
    pin::pin,
    task::{Context, Poll},
};

use rustix::{
    fd::AsFd,
    net::{AddressFamily, SocketFlags, SocketType},
};

use crate::{
    LoopStorage, Selector, io::IoRegistration, selector::Direction, testing::counting_waker,
    time::Instant,
};

type SmallStorage = LoopStorage<(), 1, 1, 64, 0, 2, 2>;

#[test]
fn clearing_drops_live_wakers() {
    let drops = Cell::new(0);
    let selector = Selector::new().unwrap();
    let ctx = {
        static STORAGE: SmallStorage = SmallStorage::new();
        STORAGE.claim(&selector).unwrap().0
    };
    let timer_waker = counting_waker(&drops);
    let _ = ctx.storage.timers.alloc(Instant::now(), &timer_waker);
    drop(timer_waker);
    let (fd, _peer) = rustix::net::socketpair(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::empty(),
        None,
    )
    .unwrap();
    let registration = IoRegistration::new(
        &ctx.storage.io_waiters,
        ctx.selector,
        fd.as_fd(),
        Direction::Read,
    )
    .unwrap();
    let io_waker = counting_waker(&drops);
    let mut wait = pin!(registration.wait());
    assert_eq!(
        wait.as_mut().poll(&mut Context::from_waker(&io_waker)),
        Poll::Pending
    );
    drop(io_waker);
    assert_eq!(drops.get(), 2);
    ctx.clear();
    assert_eq!(drops.get(), 4);
}

#[test]
fn a_storage_can_only_be_claimed_once() {
    static STORAGE: SmallStorage = SmallStorage::new();
    let selector = Selector::new().unwrap();
    assert!(STORAGE.claim(&selector).is_some());
    assert!(STORAGE.claim(&selector).is_none());
}

#[test]
fn storage_can_be_built_at_runtime() {
    let _storage = SmallStorage::new();
}
