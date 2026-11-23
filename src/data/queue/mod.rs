use core::cell::Cell;

use crate::conversions::u32_to_usize;

#[must_use]
pub(crate) struct Queue<const N: usize> {
    head: Cell<Option<u32>>,
    tail: Cell<Option<u32>>,
    links: [Cell<LinkState>; N],
    extra: Cell<LinkState>,
}

#[derive(Clone, Copy)]
enum LinkState {
    Idle,
    Queued(Option<u32>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum Popped {
    Slot(u32),
    Extra,
}

impl<const N: usize> Queue<N> {
    const EXTRA: u32 = {
        assert!(N < 0xFFFF_FFFF, "queue capacity must be less than u32::MAX");
        let [b0, b1, b2, b3, ..] = N.to_le_bytes();
        u32::from_le_bytes([b0, b1, b2, b3])
    };

    pub(crate) const fn new() -> Self {
        Self {
            head: Cell::new(None),
            tail: Cell::new(None),
            links: [const { Cell::new(LinkState::Idle) }; N],
            extra: Cell::new(LinkState::Idle),
        }
    }

    fn indexed_link(&self, idx: u32) -> &Cell<LinkState> {
        let Some(link) = self.links.get(u32_to_usize(idx)) else {
            unreachable!("slot {idx} is out of bounds");
        };
        link
    }

    fn link(&self, idx: u32) -> &Cell<LinkState> {
        if idx == Self::EXTRA {
            return &self.extra;
        }
        self.indexed_link(idx)
    }

    fn enqueue(&self, idx: u32, link: &Cell<LinkState>) {
        if let LinkState::Queued(_) = link.get() {
            return;
        }
        link.set(LinkState::Queued(None));
        match self.tail.get() {
            Some(tail) => self.link(tail).set(LinkState::Queued(Some(idx))),
            None => self.head.set(Some(idx)),
        }
        self.tail.set(Some(idx));
    }

    pub(crate) fn push(&self, idx: u32) {
        self.enqueue(idx, self.indexed_link(idx));
    }

    pub(crate) fn push_extra(&self) {
        self.enqueue(Self::EXTRA, &self.extra);
    }

    pub(crate) fn pop(&self) -> Option<Popped> {
        let idx = self.head.get()?;
        let LinkState::Queued(next) = self.link(idx).replace(LinkState::Idle) else {
            unreachable!("the head slot {idx} is not queued");
        };
        self.head.set(next);
        if next.is_none() {
            self.tail.set(None);
        }
        if idx == Self::EXTRA {
            return Some(Popped::Extra);
        }
        Some(Popped::Slot(idx))
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.head.get().is_none()
    }
}

#[cfg(test)]
mod tests;
