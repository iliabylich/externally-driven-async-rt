use core::{cell::Cell, task::Poll};

use rustix::fd::{AsRawFd, BorrowedFd};

use crate::{
    context::Context,
    io::{IoIndex, IoWaiter, IoWaiters, Wait},
    selector::{Direction, Selector},
    state::State,
};

#[must_use]
pub(crate) struct IoRegistration<'a, const FDS: usize> {
    io_waiters: &'a IoWaiters<FDS>,
    selector: &'a Selector,
    index: IoIndex,
    io: &'a IoWaiter,
    direction: Direction,
}

impl<'a, const FDS: usize> IoRegistration<'a, FDS> {
    pub(crate) fn new<
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
    >(
        ctx: &'a Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        fd: BorrowedFd<'a>,
        cached: &Cell<Option<IoIndex>>,
        direction: Direction,
    ) -> Self {
        let (index, io) = ctx
            .storage
            .io_waiters
            .register(cached, fd.as_raw_fd(), direction);
        Self {
            io_waiters: &ctx.storage.io_waiters,
            selector: ctx.selector,
            index,
            io,
            direction,
        }
    }

    pub(crate) const fn wait(&mut self) -> Wait<'_, 'a, FDS> {
        Wait::new(self)
    }

    pub(crate) fn poll_wait(&self, waiting: &mut bool, cx: &core::task::Context<'_>) -> Poll<()> {
        let waker = self.io.waker(self.direction);
        if *waiting && !waker.is_set() {
            *waiting = false;
            return Poll::Ready(());
        }
        waker.store(cx.waker());
        *waiting = true;
        Poll::Pending
    }

    pub(crate) fn cancel_wait(&self, waiting: bool) {
        if waiting {
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
