use core::{
    cell::Cell,
    task::{Poll, Waker},
};

use crate::waker_slot::WakerSlot;

#[must_use]
pub(crate) struct Notification {
    pending: Cell<bool>,
    waker: WakerSlot,
}

impl Notification {
    pub(crate) const fn new() -> Self {
        Self {
            pending: Cell::new(false),
            waker: WakerSlot::new(),
        }
    }

    pub(crate) fn fire(&self) {
        self.pending.set(true);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub(crate) fn poll(&self, waker: &Waker) -> Poll<()> {
        if self.pending.replace(false) {
            return Poll::Ready(());
        }
        self.waker.store(waker);
        Poll::Pending
    }

    pub(crate) fn forget_waker(&self) {
        drop(self.waker.take());
    }

    pub(crate) fn clear(&self) {
        self.pending.set(false);
        self.forget_waker();
    }
}
