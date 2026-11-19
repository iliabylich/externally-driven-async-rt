use core::{future::Future, pin::Pin, task::Poll};

use crate::io::IoRegistration;

#[must_use]
pub(crate) struct Wait<'r, 'a, const FDS: usize> {
    registration: &'r mut IoRegistration<'a, FDS>,
}

impl<'r, 'a, const FDS: usize> Wait<'r, 'a, FDS> {
    pub(crate) const fn new(registration: &'r mut IoRegistration<'a, FDS>) -> Self {
        Self { registration }
    }
}

impl<const FDS: usize> Future for Wait<'_, '_, FDS> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        self.get_mut().registration.poll_wait(cx)
    }
}

impl<const FDS: usize> Drop for Wait<'_, '_, FDS> {
    fn drop(&mut self) {
        self.registration.cancel_wait();
    }
}
