mod index;
mod slot;

use core::cell::Cell;

pub(crate) use self::index::VersionedIndex;
use crate::{const_unreachable::const_unreachable, conversions::u32_to_usize, helpers::element};
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

    const fn slot_at(&self, idx: u32) -> Option<SlotView<'_, T>> {
        let index = u32_to_usize(idx);
        let Some(slot) = element(&self.slots, index) else {
            return None;
        };
        let Some(value) = element(&self.values, index) else {
            return None;
        };
        Some(SlotView { idx, slot, value })
    }

    const fn free_slot_at(&self, idx: u32) -> FreeSlotView<'_, T> {
        let Some(slot) = self.slot_at(idx) else {
            const_unreachable!("the free list points outside the pool")
        };
        let Some(free) = slot.free() else {
            const_unreachable!("the free list points at an used slot")
        };
        free
    }

    const fn used_slot_at(&self, idx: u32) -> UsedSlotView<'_, T> {
        let Some(slot) = self.slot_at(idx) else {
            const_unreachable!("the used list points outside the pool")
        };
        let Some(used) = slot.used() else {
            const_unreachable!("the used list points at a vacant slot")
        };
        used
    }

    const fn free_head(&self) -> Option<FreeSlotView<'_, T>> {
        let Some(idx) = self.free_head.get() else {
            return None;
        };
        Some(self.free_slot_at(idx))
    }

    const fn used_head(&self) -> Option<UsedSlotView<'_, T>> {
        let Some(idx) = self.used_head.get() else {
            return None;
        };
        Some(self.used_slot_at(idx))
    }

    const fn used_at(&self, idx: Option<u32>) -> Option<UsedSlotView<'_, T>> {
        let Some(idx) = idx else {
            return None;
        };
        Some(self.used_slot_at(idx))
    }

    pub(crate) const fn alloc(&self) -> Option<(VersionedIndex<T>, &T)> {
        let Some(free) = self.free_head() else {
            return None;
        };

        let _ = self.free_head.replace(free.next());

        let used = free.occupy();

        UsedSlotView::link(Some(&used), self.used_head().as_ref());
        let _ = self.used_head.replace(Some(used.idx));

        Some((
            VersionedIndex::new(used.idx, used.slot.version()),
            used.value,
        ))
    }

    pub(crate) const fn free(&self, index: VersionedIndex<T>) {
        let used = self.used_slot_at(index.idx);
        if used.slot.version() != index.version {
            const_unreachable!(
                "version mismatch: {} vs {}",
                used.slot.version(),
                index.version
            );
        }

        UsedSlotView::link(
            self.used_at(used.prev()).as_ref(),
            self.used_at(used.next()).as_ref(),
        );
        if let Some(head) = self.used_head.get()
            && head == used.idx
        {
            let _ = self.used_head.replace(used.next());
        }

        let free = used.free();

        FreeSlotView::link(&free, self.free_head().as_ref());
        let _ = self.free_head.replace(Some(free.idx));
    }

    pub(crate) const fn get(&self, idx: u32) -> (VersionedIndex<T>, &T) {
        let Some(slot) = self.slot_at(idx) else {
            const_unreachable!("index {} is out of bounds", idx)
        };
        (VersionedIndex::new(idx, slot.slot.version()), slot.value)
    }

    pub(crate) const fn get_versioned(&self, index: VersionedIndex<T>) -> Option<&T> {
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
