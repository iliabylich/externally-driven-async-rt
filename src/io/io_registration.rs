use core::{cell::Cell, task::Poll};

use rustix::fd::{AsRawFd, BorrowedFd};

use crate::{
    context::Context,
    io::{FdsFullError, IoIndex, IoWaiter, IoWaiters, Wait},
    selector::{Direction, Selector},
};

#[must_use]
pub(crate) struct IoRegistration<'a, const FDS: usize> {
    io_waiters: &'a IoWaiters<FDS>,
    selector: &'a Selector,
    index: IoIndex,
    io: &'a IoWaiter,
    direction: Direction,
    waiting: bool,
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
        cached: &Cell<Option<IoIndex>>,
        direction: Direction,
    ) -> Result<Self, FdsFullError> {
        let (index, io) = ctx
            .storage
            .io_waiters
            .register(cached, fd.as_raw_fd(), direction)?;
        Ok(Self {
            io_waiters: &ctx.storage.io_waiters,
            selector: ctx.selector,
            index,
            io,
            direction,
            waiting: false,
        })
    }

    pub(crate) const fn wait(&mut self) -> Wait<'_, 'a, FDS> {
        Wait::new(self)
    }

    pub(crate) fn poll_wait(&mut self, cx: &core::task::Context<'_>) -> Poll<()> {
        let waker = self.io.waker(self.direction);
        if self.waiting && !waker.is_set() {
            self.waiting = false;
            return Poll::Ready(());
        }
        waker.store(cx.waker());
        self.waiting = true;
        Poll::Pending
    }

    pub(crate) fn cancel_wait(&mut self) {
        if self.waiting {
            self.waiting = false;
            self.io.waker(self.direction).take();
        }
    }
}

impl<const FDS: usize> Drop for IoRegistration<'_, FDS> {
    fn drop(&mut self) {
        if let Some(change) = self.io_waiters.release(self.index, self.direction) {
            let _ = self.selector.apply_now(change);
        }
    }
}
