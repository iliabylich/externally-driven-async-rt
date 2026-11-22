use core::cell::Cell;

use crate::{const_unreachable::const_unreachable, helpers::element};

#[must_use]
pub(crate) struct EventQueue<E, const N: usize> {
    slots: [Cell<Option<E>>; N],
    head: Cell<usize>,
    tail: Cell<usize>,
}

impl<E, const N: usize> EventQueue<E, N> {
    pub(crate) const fn new() -> Self {
        const {
            assert!(N > 0, "event queue capacity must not be zero");
        }
        Self {
            slots: [const { Cell::new(None) }; N],
            head: Cell::new(0),
            tail: Cell::new(0),
        }
    }

    pub(crate) const fn push(&self, event: E) -> Result<(), E> {
        let tail = self.tail.get();
        let slot = self.slot(tail);
        let previous = slot.replace(None);
        if previous.is_some() {
            let none = slot.replace(previous);
            core::mem::forget(none);
            return Err(event);
        }
        core::mem::forget(previous);
        let none = slot.replace(Some(event));
        core::mem::forget(none);
        let _ = self.tail.replace(Self::next(tail));
        Ok(())
    }

    pub(crate) const fn pop(&self) -> Option<E> {
        let head = self.head.get();
        let event = self.slot(head).replace(None);
        if event.is_some() {
            let _ = self.head.replace(Self::next(head));
        }
        event
    }

    pub(crate) fn clear(&self) {
        while self.pop().is_some() {}
    }

    const fn slot(&self, index: usize) -> &Cell<Option<E>> {
        let Some(slot) = element(&self.slots, index) else {
            const_unreachable!("queue indices stay below the capacity");
        };
        slot
    }

    const fn next(index: usize) -> usize {
        let next = index.wrapping_add(1);
        if next == N { 0 } else { next }
    }

    pub(crate) fn iter_removing(&self) -> impl Iterator<Item = E> {
        core::iter::from_fn(|| self.pop())
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::assert_yields;

    use super::EventQueue;

    #[test]
    fn pops_events_in_push_order() {
        let queue = EventQueue::<u32, 3>::new();
        assert_eq!(queue.push(1), Ok(()));
        assert_eq!(queue.push(2), Ok(()));
        assert_eq!(queue.push(3), Ok(()));
        assert_yields!(queue.iter_removing(), [1, 2, 3]);
    }

    #[test]
    fn pushing_into_a_full_queue_returns_the_event() {
        let queue = EventQueue::<u32, 2>::new();
        assert_eq!(queue.push(1), Ok(()));
        assert_eq!(queue.push(2), Ok(()));
        assert_eq!(queue.push(3), Err(3));
        assert_yields!(queue.iter_removing(), [1, 2]);
    }

    #[test]
    fn indices_wrap_around_the_capacity() {
        let queue = EventQueue::<u32, 2>::new();
        assert_eq!(queue.push(1), Ok(()));
        assert_eq!(queue.push(2), Ok(()));
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.push(3), Ok(()));
        assert_eq!(queue.push(4), Err(4));
        assert_yields!(queue.iter_removing(), [2, 3]);
    }

    #[test]
    fn zero_sized_events_are_counted() {
        let queue = EventQueue::<(), 2>::new();
        assert_eq!(queue.push(()), Ok(()));
        assert_eq!(queue.push(()), Ok(()));
        assert_eq!(queue.push(()), Err(()));
        assert_yields!(queue.iter_removing(), [(), ()]);
    }
}
