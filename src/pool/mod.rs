mod index;
mod slot;

use core::cell::Cell;

pub(crate) use self::index::VersionedIndex;
use self::slot::Slot;
use crate::conversions::u32_to_usize;

#[must_use]
pub(crate) struct ObjectPool<T, const N: usize> {
    free_head: Cell<Option<u32>>,
    occupied_head: Cell<Option<u32>>,
    slots: [Cell<Slot>; N],
    values: [T; N],
}

impl<T, const N: usize> ObjectPool<T, N> {
    pub(crate) const fn new(values: [T; N]) -> Self {
        let slots = const {
            assert!(N > 0, "object pool capacity must not be zero");
            assert!(N <= 0xFFFF_FFFF, "object pool capacity must fit in u32");
            let mut slots = [const {
                Cell::new(Slot::Vacant {
                    version: 0,
                    next_free: None,
                })
            }; N];
            let mut i = 0;
            let mut next: u32 = 1;
            while i + 1 < N {
                slots[i] = Cell::new(Slot::Vacant {
                    version: 0,
                    next_free: Some(next),
                });
                i += 1;
                next += 1;
            }
            slots
        };
        Self {
            free_head: Cell::new(Some(0)),
            occupied_head: Cell::new(None),
            slots,
            values,
        }
    }

    pub(crate) fn alloc(&self) -> Option<(VersionedIndex<T>, &T)> {
        let idx = self.free_head.get()?;
        let Some((slot, value)) = self.entry(idx) else {
            unreachable!("the free list points outside the pool");
        };
        let Slot::Vacant { version, next_free } = slot.get() else {
            unreachable!("the free list points at an occupied slot");
        };
        let head = self.occupied_head.get();
        slot.set(Slot::Occupied {
            version,
            prev: None,
            next: head,
        });
        self.free_head.set(next_free);
        if let Some(head) = head {
            self.set_prev(head, Some(idx));
        }
        self.occupied_head.set(Some(idx));
        Some((VersionedIndex::new(idx, version), value))
    }

    pub(crate) fn free(&self, idx: VersionedIndex<T>) {
        let Some((slot, _)) = self.occupied(idx) else {
            return;
        };
        let Slot::Occupied { prev, next, .. } = slot.get() else {
            unreachable!("checked by `occupied`");
        };
        slot.set(Slot::Vacant {
            version: idx.version.wrapping_add(1),
            next_free: self.free_head.get(),
        });
        match prev {
            Some(prev) => self.set_next(prev, next),
            None => self.occupied_head.set(next),
        }
        if let Some(next) = next {
            self.set_prev(next, prev);
        }
        self.free_head.set(Some(idx.idx));
    }

    fn entry(&self, idx: u32) -> Option<(&Cell<Slot>, &T)> {
        let idx = u32_to_usize(idx);
        let slot = self.slots.get(idx)?;
        let value = self.values.get(idx)?;
        Some((slot, value))
    }

    fn occupied(&self, index: VersionedIndex<T>) -> Option<(&Cell<Slot>, &T)> {
        let (slot, value) = self.entry(index.idx)?;
        match slot.get() {
            Slot::Occupied { version, .. } if version == index.version => Some((slot, value)),
            _ => None,
        }
    }

    fn set_prev(&self, idx: u32, link: Option<u32>) {
        let Some((slot, _)) = self.entry(idx) else {
            unreachable!("the occupied list points outside the pool");
        };
        let Slot::Occupied { version, next, .. } = slot.get() else {
            unreachable!("the occupied list points at a vacant slot");
        };
        slot.set(Slot::Occupied {
            version,
            prev: link,
            next,
        });
    }

    fn set_next(&self, idx: u32, link: Option<u32>) {
        let Some((slot, _)) = self.entry(idx) else {
            unreachable!("the occupied list points outside the pool");
        };
        let Slot::Occupied { version, prev, .. } = slot.get() else {
            unreachable!("the occupied list points at a vacant slot");
        };
        slot.set(Slot::Occupied {
            version,
            prev,
            next: link,
        });
    }

    pub(crate) fn get(&self, idx: VersionedIndex<T>) -> Option<&T> {
        self.occupied(idx).map(|(_, value)| value)
    }

    pub(crate) fn get_at(&self, idx: u32) -> Option<(VersionedIndex<T>, &T)> {
        let (slot, value) = self.entry(idx)?;
        let Slot::Occupied { version, .. } = slot.get() else {
            return None;
        };
        Some((VersionedIndex::new(idx, version), value))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (VersionedIndex<T>, &T)> {
        let mut cursor = self.occupied_head.get();
        core::iter::from_fn(move || {
            let idx = cursor?;
            let Some((slot, value)) = self.entry(idx) else {
                unreachable!("the occupied list points outside the pool");
            };
            let Slot::Occupied { version, next, .. } = slot.get() else {
                unreachable!("the occupied list points at a vacant slot");
            };
            cursor = next;
            Some((VersionedIndex::new(idx, version), value))
        })
    }
}

#[cfg(test)]
mod tests;
