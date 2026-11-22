use core::{cell::Cell, mem::DropGuard, task::Poll};

use rustix::fd::{AsRawFd, BorrowedFd, RawFd};

use crate::{
    context::Context,
    io::{FdsFullError, IoWaiter, IoWaiters},
    selector::{Direction, Selector},
};

#[must_use]
pub(crate) struct IoRegistration<'a, const FDS: usize> {
    io_waiters: &'a IoWaiters<FDS>,
    selector: &'a Selector,
    fd: RawFd,
    io: &'a IoWaiter,
    direction: Direction,
    waiting: Cell<bool>,
}

impl<'a, const FDS: usize> IoRegistration<'a, FDS> {
    pub(crate) fn new<
        E: 'static,
        const EVENTS: usize,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
    >(
        ctx: &'a Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        fd: BorrowedFd<'a>,
        direction: Direction,
    ) -> Result<Self, FdsFullError> {
        let fd = fd.as_raw_fd();
        let io = ctx.storage.io_waiters.register(fd, direction)?;
        Ok(Self {
            io_waiters: &ctx.storage.io_waiters,
            selector: ctx.selector,
            fd,
            io,
            direction,
            waiting: Cell::new(false),
        })
    }

    pub(crate) async fn wait(&self) {
        let guard = DropGuard::new(self, Self::cancel_wait);
        core::future::poll_fn(|cx| self.poll_wait(cx)).await;
        DropGuard::dismiss(guard);
    }

    pub(crate) fn poll_wait(&self, cx: &core::task::Context<'_>) -> Poll<()> {
        let waker = self.io.waker(self.direction);
        if self.waiting.get() && !waker.is_set() {
            self.waiting.set(false);
            return Poll::Ready(());
        }
        waker.store(cx.waker());
        self.waiting.set(true);
        Poll::Pending
    }

    fn cancel_wait(&self) {
        if self.waiting.replace(false) {
            self.io.waker(self.direction).take();
        }
    }
}

impl<const FDS: usize> Drop for IoRegistration<'_, FDS> {
    fn drop(&mut self) {
        if let Some(change) = self.io_waiters.release(self.fd, self.direction) {
            let _ = self.selector.apply_now(change);
        }
    }
}
