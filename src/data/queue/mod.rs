use core::cell::Cell;

use crate::{const_unreachable::const_unreachable, conversions::u32_to_usize, helpers::element};

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

    const fn indexed_link(&self, slot: u32) -> &Cell<LinkState> {
        let Some(link) = element(&self.links, u32_to_usize(slot)) else {
            const_unreachable!("slot {} is out of bounds", slot);
        };
        link
    }

    const fn link(&self, slot: u32) -> &Cell<LinkState> {
        if slot == Self::EXTRA {
            return &self.extra;
        }
        self.indexed_link(slot)
    }

    const fn enqueue(&self, slot: u32, link: &Cell<LinkState>) {
        if let LinkState::Queued(_) = link.get() {
            return;
        }
        let _ = link.replace(LinkState::Queued(None));
        match self.tail.get() {
            Some(tail) => {
                let _ = self.link(tail).replace(LinkState::Queued(Some(slot)));
            }
            None => {
                let _ = self.head.replace(Some(slot));
            }
        }
        let _ = self.tail.replace(Some(slot));
    }

    pub(crate) const fn push(&self, slot: u32) {
        self.enqueue(slot, self.indexed_link(slot));
    }

    pub(crate) const fn push_extra(&self) {
        self.enqueue(Self::EXTRA, &self.extra);
    }

    pub(crate) const fn pop(&self) -> Option<Popped> {
        let Some(slot) = self.head.get() else {
            return None;
        };
        let LinkState::Queued(next) = self.link(slot).replace(LinkState::Idle) else {
            const_unreachable!("the head slot {} is not queued", slot);
        };
        let _ = self.head.replace(next);
        if next.is_none() {
            let _ = self.tail.replace(None);
        }
        if slot == Self::EXTRA {
            return Some(Popped::Extra);
        }
        Some(Popped::Slot(slot))
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.head.get().is_none()
    }
}

#[cfg(test)]
mod tests;
