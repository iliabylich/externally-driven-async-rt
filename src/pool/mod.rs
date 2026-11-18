mod index;
mod slot;

use core::cell::Cell;

pub(crate) use self::index::Index;
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
                    generation: 0,
                    next_free: None,
                })
            }; N];
            let mut i = 0;
            let mut next: u32 = 1;
            while i + 1 < N {
                slots[i] = Cell::new(Slot::Vacant {
                    generation: 0,
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

    pub(crate) fn alloc(&self) -> Option<(Index<T>, &T)> {
        let slot = self.free_head.get()?;
        let Some((cell, value)) = self.entry(slot) else {
            unreachable!("the free list points outside the pool");
        };
        let Slot::Vacant {
            generation,
            next_free,
        } = cell.get()
        else {
            unreachable!("the free list points at an occupied slot");
        };
        let head = self.occupied_head.get();
        cell.set(Slot::Occupied {
            generation,
            prev: None,
            next: head,
        });
        self.free_head.set(next_free);
        if let Some(head) = head {
            self.set_prev(head, Some(slot));
        }
        self.occupied_head.set(Some(slot));
        Some((Index::from_parts(slot, generation), value))
    }

    pub(crate) fn free(&self, index: Index<T>) {
        let Some((cell, _)) = self.occupied(index) else {
            return;
        };
        let Slot::Occupied { prev, next, .. } = cell.get() else {
            unreachable!("checked by `occupied`");
        };
        cell.set(Slot::Vacant {
            generation: index.generation().wrapping_add(1),
            next_free: self.free_head.get(),
        });
        match prev {
            Some(prev) => self.set_next(prev, next),
            None => self.occupied_head.set(next),
        }
        if let Some(next) = next {
            self.set_prev(next, prev);
        }
        self.free_head.set(Some(index.slot()));
    }

    fn entry(&self, slot: u32) -> Option<(&Cell<Slot>, &T)> {
        let slot = u32_to_usize(slot);
        self.slots.get(slot).zip(self.values.get(slot))
    }

    fn occupied(&self, index: Index<T>) -> Option<(&Cell<Slot>, &T)> {
        let (cell, value) = self.entry(index.slot())?;
        match cell.get() {
            Slot::Occupied { generation, .. } if generation == index.generation() => {
                Some((cell, value))
            }
            _ => None,
        }
    }

    fn set_prev(&self, slot: u32, link: Option<u32>) {
        let Some((cell, _)) = self.entry(slot) else {
            unreachable!("the occupied list points outside the pool");
        };
        let Slot::Occupied {
            generation, next, ..
        } = cell.get()
        else {
            unreachable!("the occupied list points at a vacant slot");
        };
        cell.set(Slot::Occupied {
            generation,
            prev: link,
            next,
        });
    }

    fn set_next(&self, slot: u32, link: Option<u32>) {
        let Some((cell, _)) = self.entry(slot) else {
            unreachable!("the occupied list points outside the pool");
        };
        let Slot::Occupied {
            generation, prev, ..
        } = cell.get()
        else {
            unreachable!("the occupied list points at a vacant slot");
        };
        cell.set(Slot::Occupied {
            generation,
            prev,
            next: link,
        });
    }

    pub(crate) fn get(&self, index: Index<T>) -> Option<&T> {
        self.occupied(index).map(|(_, value)| value)
    }

    pub(crate) fn get_at(&self, slot: u32) -> Option<(Index<T>, &T)> {
        let (cell, value) = self.entry(slot)?;
        let Slot::Occupied { generation, .. } = cell.get() else {
            return None;
        };
        Some((Index::from_parts(slot, generation), value))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Index<T>, &T)> {
        let mut cursor = self.occupied_head.get();
        core::iter::from_fn(move || {
            let slot = cursor?;
            let Some((cell, value)) = self.entry(slot) else {
                unreachable!("the occupied list points outside the pool");
            };
            let Slot::Occupied {
                generation, next, ..
            } = cell.get()
            else {
                unreachable!("the occupied list points at a vacant slot");
            };
            cursor = next;
            Some((Index::from_parts(slot, generation), value))
        })
    }
}

#[cfg(test)]
mod tests;
