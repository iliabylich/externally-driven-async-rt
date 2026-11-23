use core::cell::Cell;

use crate::conversions::u32_to_usize;

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

    fn heap_idx(&self, pos: usize) -> &Cell<u32> {
        let Some(idx) = self.heap.get(pos) else {
            unreachable!("heap position {pos} is out of bounds");
        };
        idx
    }

    fn idx_at(&self, pos: usize) -> u32 {
        self.heap_idx(pos).get()
    }

    fn key(&self, idx: u32) -> &Cell<Option<K>> {
        let Some(key) = self.keys.get(u32_to_usize(idx)) else {
            unreachable!("slot {idx} is out of bounds");
        };
        key
    }

    fn position(&self, idx: u32) -> &Cell<usize> {
        let Some(position) = self.positions.get(u32_to_usize(idx)) else {
            unreachable!("slot {idx} is out of bounds");
        };
        position
    }

    fn key_at(&self, pos: usize) -> K {
        let idx = self.idx_at(pos);
        let Some(key) = self.key(idx).get() else {
            unreachable!("slot {idx} at heap position {pos} has no key");
        };
        key
    }

    fn place(&self, pos: usize, idx: u32) {
        self.heap_idx(pos).set(idx);
        self.position(idx).set(pos);
    }

    fn swap(&self, a: usize, b: usize) {
        let idx_a = self.idx_at(a);
        let idx_b = self.idx_at(b);
        self.place(a, idx_b);
        self.place(b, idx_a);
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

    pub(crate) fn contains(&self, idx: u32) -> bool {
        self.key(idx).get().is_some()
    }

    pub(crate) fn insert(&self, idx: u32, key: K) {
        if self.key(idx).replace(Some(key)).is_some() {
            unreachable!("slot {idx} is already in the heap");
        }
        let pos = self.len.get();
        self.place(pos, idx);
        self.len.set(pos.wrapping_add(1));
        self.sift_up(pos);
    }

    pub(crate) fn remove(&self, idx: u32) {
        if self.key(idx).get().is_none() {
            unreachable!("slot {idx} is not in the heap");
        }
        let pos = self.position(idx).get();
        let last = self.len.get().wrapping_sub(1);
        self.swap(pos, last);
        self.len.set(last);
        self.key(idx).set(None);
        if pos < last {
            self.sift_down(pos);
            self.sift_up(pos);
        }
    }

    pub(crate) fn peek(&self) -> Option<(u32, K)> {
        if self.len.get() == 0 {
            return None;
        }
        Some((self.idx_at(0), self.key_at(0)))
    }

    pub(crate) fn clear(&self) {
        for pos in 0..self.len.get() {
            self.key(self.idx_at(pos)).set(None);
        }
        self.len.set(0);
    }
}

#[cfg(test)]
mod tests;
