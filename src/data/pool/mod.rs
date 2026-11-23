mod index;
mod slot;

use core::cell::Cell;

pub(crate) use self::index::VersionedIndex;
use crate::conversions::u32_to_usize;
use slot::{FreeSlotView, Slot, SlotView, UsedSlotView};

#[must_use]
pub(crate) struct ObjectPool<T, const N: usize> {
    free_head: Cell<Option<u32>>,
    used_head: Cell<Option<u32>>,
    slots: [Slot; N],
    values: [T; N],
}

impl<T, const N: usize> ObjectPool<T, N> {
    pub(crate) const fn new(values: [T; N]) -> Self {
        Self {
            free_head: Cell::new(Some(0)),
            used_head: Cell::new(None),
            slots: const {
                assert!(N > 0, "object pool capacity must not be zero");
                assert!(N <= 0xFFFF_FFFF, "object pool capacity must fit in u32");
                Slot::const_list()
            },
            values,
        }
    }

    fn slot_at(&self, idx: u32) -> Option<SlotView<'_, T>> {
        let index = u32_to_usize(idx);
        let slot = self.slots.get(index)?;
        let value = self.values.get(index)?;
        Some(SlotView { idx, slot, value })
    }

    fn free_slot_at(&self, idx: u32) -> FreeSlotView<'_, T> {
        let Some(slot) = self.slot_at(idx) else {
            unreachable!("the free list points outside the pool")
        };
        let Some(free) = slot.free() else {
            unreachable!("the free list points at an used slot")
        };
        free
    }

    fn used_slot_at(&self, idx: u32) -> UsedSlotView<'_, T> {
        let Some(slot) = self.slot_at(idx) else {
            unreachable!("the used list points outside the pool")
        };
        let Some(used) = slot.used() else {
            unreachable!("the used list points at a vacant slot")
        };
        used
    }

    fn free_head(&self) -> Option<FreeSlotView<'_, T>> {
        let idx = self.free_head.get()?;
        Some(self.free_slot_at(idx))
    }

    fn used_head(&self) -> Option<UsedSlotView<'_, T>> {
        let idx = self.used_head.get()?;
        Some(self.used_slot_at(idx))
    }

    fn used_at(&self, idx: Option<u32>) -> Option<UsedSlotView<'_, T>> {
        let idx = idx?;
        Some(self.used_slot_at(idx))
    }

    pub(crate) fn alloc(&self) -> Option<(VersionedIndex<T>, &T)> {
        let free = self.free_head()?;
        self.free_head.set(free.next());

        let used = free.occupy();

        UsedSlotView::link(Some(&used), self.used_head().as_ref());
        self.used_head.set(Some(used.idx));

        Some((
            VersionedIndex::new(used.idx, used.slot.version()),
            used.value,
        ))
    }

    pub(crate) fn free(&self, index: VersionedIndex<T>) {
        let used = self.used_slot_at(index.idx);
        if used.slot.version() != index.version {
            unreachable!(
                "version mismatch: {} vs {}",
                used.slot.version(),
                index.version
            );
        }

        UsedSlotView::link(
            self.used_at(used.prev()).as_ref(),
            self.used_at(used.next()).as_ref(),
        );
        if self.used_head.get() == Some(used.idx) {
            self.used_head.set(used.next());
        }

        let free = used.free();

        FreeSlotView::link(&free, self.free_head().as_ref());
        self.free_head.set(Some(free.idx));
    }

    pub(crate) fn get(&self, idx: u32) -> (VersionedIndex<T>, &T) {
        let Some(slot) = self.slot_at(idx) else {
            unreachable!("index {idx} is out of bounds")
        };
        (VersionedIndex::new(idx, slot.slot.version()), slot.value)
    }

    pub(crate) fn get_versioned(&self, index: VersionedIndex<T>) -> Option<&T> {
        let (current, value) = self.get(index.idx);
        if current.version == index.version {
            return Some(value);
        }
        None
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (VersionedIndex<T>, &T)> {
        let mut cursor = self.used_head.get();
        core::iter::from_fn(move || {
            let idx = cursor?;
            let used = self.used_slot_at(idx);
            cursor = used.next();
            Some((VersionedIndex::new(idx, used.slot.version()), used.value))
        })
    }
}

#[cfg(test)]
mod tests;
