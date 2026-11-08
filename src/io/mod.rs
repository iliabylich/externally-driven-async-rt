use core::{future::Future, pin::Pin, task::Poll};

use rustix::fd::BorrowedFd;

use crate::{
    selectors::Direction,
    storage::{IoIndex, Storage},
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
    storage: &'a Storage,
    index: IoIndex,
}

impl<'a> IoRegistration<'a> {
    pub(crate) fn new(storage: &'a Storage, fd: BorrowedFd<'a>, direction: Direction) -> Self {
        Self {
            storage,
            index: storage.register_io(fd, direction),
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
        let _ = self.storage.deregister_io(self.index);
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
        let IoRegistration { storage, index } = *this.registration;
        if this.waiting && !storage.has_io_waker(index) {
            this.waiting = false;
            return Poll::Ready(());
        }
        storage.set_io_waker(index, cx.waker());
        this.waiting = true;
        Poll::Pending
    }
}

impl Drop for Wait<'_, '_> {
    fn drop(&mut self) {
        if self.waiting {
            let IoRegistration { storage, index } = *self.registration;
            storage.remove_io_waker(index);
        }
    }
}

#[cfg(test)]
mod tests;
