use core::{future::Future, pin::Pin, task::Poll};

use crate::io::IoRegistration;

pub(crate) struct Wait<'r, 'a, const FDS: usize> {
    registration: &'r mut IoRegistration<'a, FDS>,
    waiting: bool,
}

impl<'r, 'a, const FDS: usize> Wait<'r, 'a, FDS> {
    pub(crate) fn new(registration: &'r mut IoRegistration<'a, FDS>) -> Self {
        Self {
            registration,
            waiting: false,
        }
    }
}

impl<const FDS: usize> Future for Wait<'_, '_, FDS> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        this.registration.poll_wait(&mut this.waiting, cx)
    }
}

impl<const FDS: usize> Drop for Wait<'_, '_, FDS> {
    fn drop(&mut self) {
        self.registration.cancel_wait(self.waiting);
    }
}
