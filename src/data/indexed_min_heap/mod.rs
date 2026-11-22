use core::cell::Cell;

use crate::{const_unreachable::const_unreachable, conversions::u32_to_usize, helpers::element};

#[must_use]
pub(crate) struct IndexedMinHeap<K, const N: usize> {
    len: Cell<usize>,
    heap: [Cell<u32>; N],
    keys: [Cell<Option<K>>; N],
    positions: [Cell<usize>; N],
}

impl<K: Ord + Copy, const N: usize> IndexedMinHeap<K, N> {
    pub(crate) const fn new() -> Self {
        Self {
            len: Cell::new(0),
            heap: [const { Cell::new(0) }; N],
            keys: [const { Cell::new(None) }; N],
            positions: [const { Cell::new(0) }; N],
        }
    }

    const fn slot_at(&self, pos: usize) -> u32 {
        let Some(slot) = element(&self.heap, pos) else {
            const_unreachable!("heap position {} is out of bounds", pos);
        };
        slot.get()
    }

    const fn key_cell(&self, slot: u32) -> &Cell<Option<K>> {
        let Some(key) = element(&self.keys, u32_to_usize(slot)) else {
            const_unreachable!("slot {} is out of bounds", slot);
        };
        key
    }

    const fn position_cell(&self, slot: u32) -> &Cell<usize> {
        let Some(position) = element(&self.positions, u32_to_usize(slot)) else {
            const_unreachable!("slot {} is out of bounds", slot);
        };
        position
    }

    const fn key_at(&self, pos: usize) -> K {
        let slot = self.slot_at(pos);
        let Some(key) = self.key_cell(slot).get() else {
            const_unreachable!("slot {} at heap position {} has no key", slot, pos);
        };
        key
    }

    const fn place(&self, pos: usize, slot: u32) {
        let Some(cell) = element(&self.heap, pos) else {
            const_unreachable!("heap position {} is out of bounds", pos);
        };
        let _ = cell.replace(slot);
        let _ = self.position_cell(slot).replace(pos);
    }

    const fn swap(&self, a: usize, b: usize) {
        let slot_a = self.slot_at(a);
        let slot_b = self.slot_at(b);
        self.place(a, slot_b);
        self.place(b, slot_a);
    }

    fn sift_up(&self, mut pos: usize) {
        while pos > 0 {
            let parent = pos.wrapping_sub(1) / 2;
            if self.key_at(pos) >= self.key_at(parent) {
                return;
            }
            self.swap(pos, parent);
            pos = parent;
        }
    }

    fn sift_down(&self, mut pos: usize) {
        let len = self.len.get();
        loop {
            let left = pos.wrapping_mul(2).wrapping_add(1);
            let right = left.wrapping_add(1);
            let mut smallest = pos;
            if left < len && self.key_at(left) < self.key_at(smallest) {
                smallest = left;
            }
            if right < len && self.key_at(right) < self.key_at(smallest) {
                smallest = right;
            }
            if smallest == pos {
                return;
            }
            self.swap(pos, smallest);
            pos = smallest;
        }
    }

    pub(crate) const fn contains(&self, slot: u32) -> bool {
        self.key_cell(slot).get().is_some()
    }

    pub(crate) fn insert(&self, slot: u32, key: K) {
        if self.key_cell(slot).replace(Some(key)).is_some() {
            const_unreachable!("slot {} is already in the heap", slot);
        }
        let pos = self.len.get();
        self.place(pos, slot);
        let _ = self.len.replace(pos.wrapping_add(1));
        self.sift_up(pos);
    }

    pub(crate) fn remove(&self, slot: u32) {
        if self.key_cell(slot).get().is_none() {
            const_unreachable!("slot {} is not in the heap", slot);
        }
        let pos = self.position_cell(slot).get();
        let last = self.len.get().wrapping_sub(1);
        self.swap(pos, last);
        let _ = self.len.replace(last);
        let _ = self.key_cell(slot).replace(None);
        if pos < last {
            self.sift_down(pos);
            self.sift_up(pos);
        }
    }

    pub(crate) const fn peek(&self) -> Option<(u32, K)> {
        if self.len.get() == 0 {
            return None;
        }
        Some((self.slot_at(0), self.key_at(0)))
    }

    pub(crate) const fn clear(&self) {
        let mut pos = self.len.get();
        while pos > 0 {
            pos = pos.wrapping_sub(1);
            let _ = self.key_cell(self.slot_at(pos)).replace(None);
        }
        let _ = self.len.replace(0);
    }
}

#[cfg(test)]
mod tests;
