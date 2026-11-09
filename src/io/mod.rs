use core::{future::Future, pin::Pin, task::Poll};

use rustix::fd::{AsRawFd, BorrowedFd};

use crate::{
    selectors::{Direction, Selector},
    state::State,
    storage::{IoIndex, IoWaiters, StorageGuard},
};

pub(crate) struct IoRegistration<'a, const MAX_IO_COUNT: usize> {
    io_waiters: &'a IoWaiters<MAX_IO_COUNT>,
    selector: &'a Selector,
    index: IoIndex,
}

impl<'a, const MAX_IO_COUNT: usize> IoRegistration<'a, MAX_IO_COUNT> {
    pub(crate) fn new<
        S: State,
        const MAX_TASKS_COUNT: usize,
        const MAX_TASK_SIZE: usize,
        const MAX_ROOT_TASK_SIZE: usize,
        const MAX_TIMERS_COUNT: usize,
    >(
        storage: &'a StorageGuard<
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
            io_waiters: storage.io_waiters,
            selector: storage.selector(),
            index: storage.io_waiters.register(fd.as_raw_fd(), direction),
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

pub(crate) struct Wait<'r, 'a, const MAX_IO_COUNT: usize> {
    registration: &'r mut IoRegistration<'a, MAX_IO_COUNT>,
    waiting: bool,
}

impl<const MAX_IO_COUNT: usize> Future for Wait<'_, '_, MAX_IO_COUNT> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let IoRegistration {
            io_waiters, index, ..
        } = *this.registration;
        if this.waiting && !io_waiters.has_waker(index) {
            this.waiting = false;
            return Poll::Ready(());
        }
        io_waiters.set_waker(index, cx.waker());
        this.waiting = true;
        Poll::Pending
    }
}

impl<const MAX_IO_COUNT: usize> Drop for Wait<'_, '_, MAX_IO_COUNT> {
    fn drop(&mut self) {
        if self.waiting {
            self.registration
                .io_waiters
                .remove_waker(self.registration.index);
        }
    }
}

#[cfg(test)]
mod tests;
