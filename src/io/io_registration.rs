use core::{mem::DropGuard, task::Poll};

use rustix::fd::{AsRawFd, BorrowedFd, RawFd};

use crate::{
    NotEnoughResources,
    io::{IoWaiter, IoWaiters},
    selector::{Direction, Selector},
    waker_slot::WakerSlot,
};

#[must_use]
pub(crate) struct IoRegistration<'a, const FDS: usize> {
    io_waiters: &'a IoWaiters<FDS>,
    selector: &'a Selector,
    fd: RawFd,
    io: &'a IoWaiter,
    direction: Direction,
}

impl<'a, const FDS: usize> IoRegistration<'a, FDS> {
    pub(crate) fn new(
        io_waiters: &'a IoWaiters<FDS>,
        selector: &'a Selector,
        fd: BorrowedFd<'a>,
        direction: Direction,
    ) -> Result<Self, NotEnoughResources> {
        let fd = fd.as_raw_fd();
        let io = io_waiters.register(fd, direction)?;
        Ok(Self {
            io_waiters,
            selector,
            fd,
            io,
            direction,
        })
    }

    pub(crate) async fn wait(&self) {
        let waker = self.io.waker(self.direction);
        let guard = DropGuard::new(waker, |waker: &WakerSlot| drop(waker.take()));
        let mut stored = false;
        core::future::poll_fn(|cx| {
            if stored && !waker.is_set() {
                return Poll::Ready(());
            }
            waker.store(cx.waker());
            stored = true;
            Poll::Pending
        })
        .await;
        DropGuard::dismiss(guard);
    }
}

impl<const FDS: usize> Drop for IoRegistration<'_, FDS> {
    fn drop(&mut self) {
        if let Some(change) = self.io_waiters.release(self.fd, self.direction) {
            let _ = self.selector.apply_now(change);
        }
    }
}
