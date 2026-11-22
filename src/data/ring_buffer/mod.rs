use core::cell::Cell;

use crate::{const_unreachable::const_unreachable, helpers::element};

#[must_use]
pub(crate) struct RingBuffer<T, const N: usize> {
    slots: [Cell<Option<T>>; N],
    head: Cell<usize>,
    tail: Cell<usize>,
}

impl<T, const N: usize> RingBuffer<T, N> {
    pub(crate) const fn new() -> Self {
        const {
            assert!(N > 0, "ring buffer capacity must not be zero");
        }
        Self {
            slots: [const { Cell::new(None) }; N],
            head: Cell::new(0),
            tail: Cell::new(0),
        }
    }

    pub(crate) const fn push(&self, value: T) -> Result<(), T> {
        let tail = self.tail.get();
        let slot = self.slot(tail);
        let previous = slot.replace(None);
        if previous.is_some() {
            let none = slot.replace(previous);
            core::mem::forget(none);
            return Err(value);
        }
        core::mem::forget(previous);
        let none = slot.replace(Some(value));
        core::mem::forget(none);
        let _ = self.tail.replace(Self::next(tail));
        Ok(())
    }

    pub(crate) const fn pop(&self) -> Option<T> {
        let head = self.head.get();
        let value = self.slot(head).replace(None);
        if value.is_some() {
            let _ = self.head.replace(Self::next(head));
        }
        value
    }

    pub(crate) fn clear(&self) {
        while self.pop().is_some() {}
    }

    const fn slot(&self, index: usize) -> &Cell<Option<T>> {
        let Some(slot) = element(&self.slots, index) else {
            const_unreachable!("ring buffer indices stay below the capacity");
        };
        slot
    }

    const fn next(index: usize) -> usize {
        let next = index.wrapping_add(1);
        if next == N { 0 } else { next }
    }

    pub(crate) fn iter_removing(&self) -> impl Iterator<Item = T> {
        core::iter::from_fn(|| self.pop())
    }
}

#[cfg(test)]
mod tests;
