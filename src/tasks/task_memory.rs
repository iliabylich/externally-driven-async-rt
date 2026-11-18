use core::{cell::Cell, ptr::NonNull};

use crate::tasks::Aligned16;

#[must_use]
pub(crate) struct TaskMemory<const SIZE: usize> {
    occupied: Cell<bool>,
    bytes: Aligned16<SIZE>,
}

impl<const SIZE: usize> TaskMemory<SIZE> {
    pub(crate) const fn new() -> Self {
        Self {
            occupied: Cell::new(false),
            bytes: Aligned16::new(),
        }
    }

    pub(crate) const fn occupied(&self) -> &Cell<bool> {
        &self.occupied
    }

    pub(crate) fn as_ptr(&self) -> NonNull<u8> {
        self.bytes.as_ptr()
    }
}
