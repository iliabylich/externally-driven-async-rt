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

pub(crate) struct IoRegistration<'a, const MAX_IO_COUNT: usize> {
    io_waiters: &'a IoWaiters<MAX_IO_COUNT>,
    selector: &'a Selector,
    index: IoIndex,
    direction: Direction,
}

impl<'a, const MAX_IO_COUNT: usize> IoRegistration<'a, MAX_IO_COUNT> {
    pub(crate) fn new<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
    >(
        storage: &'a LoopStorageGuard<
            '_,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
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

    pub(crate) fn wait(&mut self) -> Wait<'_, 'a, MAX_IO_COUNT> {
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

impl<const MAX_IO_COUNT: usize> Drop for IoRegistration<'_, MAX_IO_COUNT> {
    fn drop(&mut self) {
        if let Some(change) = self.io_waiters.release(self.index, self.direction) {
            let _ = self.selector.apply_now(change);
        }
    }
}
