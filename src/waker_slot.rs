use core::{cell::Cell, task::Waker};

#[must_use]
pub(crate) struct WakerSlot(Cell<Option<Waker>>);

impl WakerSlot {
    pub(crate) const fn new() -> Self {
        Self(Cell::new(None))
    }

    pub(crate) fn store(&self, waker: &Waker) {
        if let Some(mut stored) = self.0.take() {
            stored.clone_from(waker);
            self.0.set(Some(stored));
        } else {
            let waker = waker.clone();
            self.0.set(Some(waker));
        }
    }

    pub(crate) fn wake_by_ref(&self) {
        if let Some(waker) = self.0.replace(None) {
            waker.wake_by_ref();
            self.0.set(Some(waker));
        }
    }

    pub(crate) const fn take(&self) -> Option<Waker> {
        self.0.replace(None)
    }

    pub(crate) const fn is_set(&self) -> bool {
        let waker = self.0.replace(None);
        let is_set = waker.is_some();
        let none = self.0.replace(waker);
        core::mem::forget(none);
        is_set
    }
}
