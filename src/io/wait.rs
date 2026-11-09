use core::{future::Future, pin::Pin, task::Poll};

use crate::io::IoRegistration;

pub(crate) struct Wait<'r, 'a, const MAX_IO_COUNT: usize> {
    pub(super) registration: &'r mut IoRegistration<'a, MAX_IO_COUNT>,
    pub(super) waiting: bool,
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
