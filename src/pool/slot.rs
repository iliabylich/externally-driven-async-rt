use crate::conversions::u32_to_usize;
use core::cell::Cell;

pub(crate) struct Slot {
    used: Cell<bool>,
    version: Cell<u16>,
    prev: Cell<Option<u32>>,
    next: Cell<Option<u32>>,
}

impl Slot {
    const fn new() -> Self {
        Self {
            used: Cell::new(false),
            version: Cell::new(0),
            prev: Cell::new(None),
            next: Cell::new(None),
        }
    }

    pub(crate) const fn version(&self) -> u16 {
        self.version.get()
    }

    const fn reset(&self, used: bool) -> u16 {
        self.used.replace(used);
        self.version.replace(self.version.get().wrapping_add(1));
        self.prev.replace(None);
        self.next.replace(None);
        self.version.get()
    }

    pub(crate) const fn const_list<const N: usize>() -> [Self; N] {
        const {
            let slots = [const { Self::new() }; N];
            let mut left_idx = 0u32;

            while u32_to_usize(left_idx + 1) < N {
                slots[u32_to_usize(left_idx)]
                    .next
                    .replace(Some(left_idx + 1));
                left_idx += 1;
            }
            slots
        }
    }
}

pub(crate) struct SlotView<'a, T> {
    pub(crate) idx: u32,
    pub(crate) slot: &'a Slot,
    pub(crate) value: &'a T,
}

impl<'a, T> SlotView<'a, T> {
    pub(crate) const fn free(self) -> Option<FreeSlotView<'a, T>> {
        let Self { idx, slot, value } = self;
        if slot.used.get() {
            return None;
        }
        Some(FreeSlotView { idx, value, slot })
    }

    pub(crate) const fn used(self) -> Option<UsedSlotView<'a, T>> {
        let Self { idx, slot, value } = self;
        if !slot.used.get() {
            return None;
        }
        Some(UsedSlotView { idx, value, slot })
    }
}

pub(crate) struct FreeSlotView<'a, T> {
    pub(crate) idx: u32,
    pub(crate) value: &'a T,
    pub(crate) slot: &'a Slot,
}

impl<'a, T> FreeSlotView<'a, T> {
    pub(crate) const fn occupy(self) -> UsedSlotView<'a, T> {
        self.slot.reset(true);
        let Self { idx, slot, value } = self;
        UsedSlotView { idx, value, slot }
    }

    pub(crate) const fn next(&self) -> Option<u32> {
        self.slot.next.get()
    }

    pub(crate) fn link(prev: &Self, next: Option<&Self>) {
        prev.slot.next.set(next.map(|v| v.idx));
    }
}

pub(crate) struct UsedSlotView<'a, T> {
    pub(crate) idx: u32,
    pub(crate) value: &'a T,
    pub(crate) slot: &'a Slot,
}

impl<'a, T> UsedSlotView<'a, T> {
    pub(crate) const fn free(self) -> FreeSlotView<'a, T> {
        self.slot.reset(false);

        FreeSlotView {
            idx: self.idx,
            value: self.value,
            slot: self.slot,
        }
    }
    pub(crate) fn link(prev: Option<&Self>, next: Option<&Self>) {
        if let Some(prev) = prev {
            prev.slot.next.set(next.map(|v| v.idx));
        }

        if let Some(next) = next {
            next.slot.prev.set(prev.map(|v| v.idx));
        }
    }

    pub(crate) const fn prev(&self) -> Option<u32> {
        self.slot.prev.get()
    }

    pub(crate) const fn next(&self) -> Option<u32> {
        self.slot.next.get()
    }
}
