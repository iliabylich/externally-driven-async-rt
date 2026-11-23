use core::cell::Cell;

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

    pub(crate) fn push(&self, value: T) -> Result<(), T> {
        let tail = self.tail.get();
        let slot = self.slot(tail);
        let previous = slot.take();
        if previous.is_some() {
            slot.set(previous);
            return Err(value);
        }
        slot.set(Some(value));
        self.tail.set(Self::next(tail));
        Ok(())
    }

    pub(crate) fn pop(&self) -> Option<T> {
        let head = self.head.get();
        let value = self.slot(head).take();
        if value.is_some() {
            self.head.set(Self::next(head));
        }
        value
    }

    pub(crate) fn clear(&self) {
        while self.pop().is_some() {}
    }

    fn slot(&self, index: usize) -> &Cell<Option<T>> {
        let Some(slot) = self.slots.get(index) else {
            unreachable!("ring buffer indices stay below the capacity");
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
