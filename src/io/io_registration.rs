use core::{
    cell::Cell,
    task::{Context, Poll},
};

use rustix::fd::{AsRawFd, BorrowedFd};

use crate::{
    io::{IoIndex, IoWaiters, Wait},
    selector::{Direction, Selector},
    state::State,
    storage::LoopStorageGuard,
};

pub(crate) struct IoRegistration<'a, const FDS: usize> {
    io_waiters: &'a IoWaiters<FDS>,
    selector: &'a Selector,
    index: IoIndex,
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
        storage: &'a LoopStorageGuard<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        fd: BorrowedFd<'a>,
        cached: &Cell<Option<IoIndex>>,
        direction: Direction,
    ) -> Self {
        Self {
            io_waiters: storage.io_waiters(),
            selector: storage.selector(),
            index: storage
                .io_waiters()
                .register(cached, fd.as_raw_fd(), direction),
            direction,
        }
    }

    pub(crate) fn wait(&mut self) -> Wait<'_, 'a, FDS> {
        Wait::new(self)
    }

    pub(crate) fn poll_wait(&self, waiting: &mut bool, cx: &mut Context<'_>) -> Poll<()> {
        if *waiting && !self.io_waiters.has_waker(self.index, self.direction) {
            *waiting = false;
            return Poll::Ready(());
        }
        self.io_waiters
            .set_waker(self.index, self.direction, cx.waker());
        *waiting = true;
        Poll::Pending
    }

    pub(crate) fn cancel_wait(&self, waiting: bool) {
        if waiting {
            self.io_waiters.remove_waker(self.index, self.direction);
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
