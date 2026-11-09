use rustix::fd::{AsRawFd, BorrowedFd};

use crate::{
    io::{IoIndex, IoWaiters, Wait},
    selector::{Direction, Selector},
    state::State,
    storage::LoopStorageGuard,
};

pub(crate) struct IoRegistration<'a, const MAX_IO_COUNT: usize> {
    pub(super) io_waiters: &'a IoWaiters<MAX_IO_COUNT>,
    selector: &'a Selector,
    pub(super) index: IoIndex,
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
        direction: Direction,
    ) -> Self {
        Self {
            io_waiters: storage.io_waiters(),
            selector: storage.selector(),
            index: storage.io_waiters().register(fd.as_raw_fd(), direction),
        }
    }

    pub(crate) fn wait(&mut self) -> Wait<'_, 'a, MAX_IO_COUNT> {
        Wait {
            registration: self,
            waiting: false,
        }
    }
}

impl<const MAX_IO_COUNT: usize> Drop for IoRegistration<'_, MAX_IO_COUNT> {
    fn drop(&mut self) {
        if let Some(change) = self.io_waiters.release(self.index) {
            let _ = self.selector.apply_now(change);
        }
    }
}
