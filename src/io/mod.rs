use core::{future::Future, pin::Pin, task::Poll};

use rustix::fd::{AsRawFd, BorrowedFd};

use crate::{
    selectors::{Direction, Selector},
    state::State,
    storage::{IoIndex, IoWaiters, Storage},
};

// Runs a non-blocking syscall until it doesn't return `EAGAIN`, waiting for `$direction`
// readiness of `$fd` in between. Must be used inside an `async` block.
macro_rules! io_loop {
    ($ctx:expr, $fd:expr, $direction:expr, $syscall:expr) => {{
        let mut registration = $crate::io::IoRegistration::new($ctx.storage(), $fd, $direction);
        loop {
            match $syscall {
                Err(rustix::io::Errno::INTR) => {}
                Err(rustix::io::Errno::AGAIN) => registration.wait().await,
                result => break result,
            }
        }
    }};
}

pub(crate) use io_loop;

pub(crate) struct IoRegistration<'a> {
    io_waiters: &'a IoWaiters,
    selector: &'a Selector,
    index: IoIndex,
}

impl<'a> IoRegistration<'a> {
    pub(crate) fn new<S: State>(
        storage: &'a Storage<S>,
        fd: BorrowedFd<'a>,
        direction: Direction,
    ) -> Self {
        Self {
            io_waiters: &storage.io_waiters,
            selector: storage.selector(),
            index: storage.io_waiters.register(fd.as_raw_fd(), direction),
        }
    }

    pub(crate) fn wait(&mut self) -> Wait<'_, 'a> {
        Wait {
            registration: self,
            waiting: false,
        }
    }
}

impl Drop for IoRegistration<'_> {
    fn drop(&mut self) {
        // On failure the slot is released anyway, so a filter left armed can only produce one
        // ignored event (its index no longer matches) until the fd is closed.
        if let Some(change) = self.io_waiters.release(self.index) {
            let _ = self.selector.apply_now(change);
        }
    }
}

pub(crate) struct Wait<'r, 'a> {
    registration: &'r mut IoRegistration<'a>,
    waiting: bool,
}

impl Future for Wait<'_, '_> {
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

impl Drop for Wait<'_, '_> {
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
