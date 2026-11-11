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
            self.set(stored);
        } else {
            self.set(waker.clone());
        }
    }

    pub(crate) fn set(&self, waker: Waker) {
        self.0.set(Some(waker));
    }

    pub(crate) fn wake_by_ref(&self) {
        if let Some(waker) = self.0.take() {
            waker.wake_by_ref();
            self.0.set(Some(waker));
        }
    }

    pub(crate) fn take(&self) -> Option<Waker> {
        self.0.take()
    }

    pub(crate) fn is_set(&self) -> bool {
        let waker = self.0.take();
        let is_set = waker.is_some();
        self.0.set(waker);
        is_set
    }
}
