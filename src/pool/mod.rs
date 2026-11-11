mod index;
mod links;
mod meta;

use core::cell::Cell;

pub(crate) use self::index::Index;
use self::{links::Links, meta::Meta};
use crate::conversions::u32_to_usize;

#[must_use]
pub(crate) struct ObjectPool<T, const N: usize> {
    free_head: Cell<Option<u32>>,
    occupied_head: Cell<Option<u32>>,
    meta: [Meta; N],
    values: [T; N],
}

impl<T, const N: usize> ObjectPool<T, N> {
    pub(crate) const fn new(values: [T; N]) -> Self {
        const {
            assert!(N > 0, "object pool capacity must not be zero");
            assert!(N <= 0xFFFF_FFFF, "object pool capacity must fit in u32");
        }
        let mut meta = [const { Meta::new(None) }; N];
        let mut rest: &mut [Meta] = &mut meta;
        let mut next: u32 = 1;
        while let [first, tail @ ..] = rest
            && !tail.is_empty()
        {
            *first = Meta::new(Some(next));
            next = next.wrapping_add(1);
            rest = tail;
        }
        Self {
            free_head: Cell::new(Some(0)),
            occupied_head: Cell::new(None),
            meta,
            values,
        }
    }

    pub(crate) fn alloc(&self) -> Option<(Index<T>, &T)> {
        let slot = self.free_head.get()?;
        let Some((meta, value)) = self.entry(slot) else {
            unreachable!("the free list points outside the pool");
        };
        let Links::Vacant { next_free } = meta.links() else {
            unreachable!("the free list points at an occupied slot");
        };
        let head = self.occupied_head.get();
        meta.set_links(Links::Occupied {
            prev: None,
            next: head,
        });
        self.free_head.set(next_free);
        if let Some(head) = head {
            self.set_prev(head, Some(slot));
        }
        self.occupied_head.set(Some(slot));
        Some((Index::from_parts(slot, meta.generation()), value))
    }

    pub(crate) fn free(&self, index: Index<T>) {
        let Some((meta, _)) = self.occupied(index) else {
            return;
        };
        let Links::Occupied { prev, next } = meta.links() else {
            unreachable!("checked by `occupied`");
        };
        meta.set_generation(index.generation().wrapping_add(1));
        meta.set_links(Links::Vacant {
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

    fn entry(&self, slot: u32) -> Option<(&Meta, &T)> {
        let slot = u32_to_usize(slot);
        self.meta.get(slot).zip(self.values.get(slot))
    }

    fn occupied(&self, index: Index<T>) -> Option<(&Meta, &T)> {
        let (meta, value) = self.entry(index.slot())?;
        let occupied = matches!(meta.links(), Links::Occupied { .. });
        if occupied && meta.generation() == index.generation() {
            Some((meta, value))
        } else {
            None
        }
    }

    fn set_prev(&self, slot: u32, link: Option<u32>) {
        let Some((meta, _)) = self.entry(slot) else {
            unreachable!("the occupied list points outside the pool");
        };
        let Links::Occupied { next, .. } = meta.links() else {
            unreachable!("the occupied list points at a vacant slot");
        };
        meta.set_links(Links::Occupied { prev: link, next });
    }

    fn set_next(&self, slot: u32, link: Option<u32>) {
        let Some((meta, _)) = self.entry(slot) else {
            unreachable!("the occupied list points outside the pool");
        };
        let Links::Occupied { prev, .. } = meta.links() else {
            unreachable!("the occupied list points at a vacant slot");
        };
        meta.set_links(Links::Occupied { prev, next: link });
    }

    pub(crate) fn get(&self, index: Index<T>) -> Option<&T> {
        self.occupied(index).map(|(_, value)| value)
    }

    pub(crate) fn get_at(&self, slot: u32) -> Option<(Index<T>, &T)> {
        let (meta, value) = self.entry(slot)?;
        if !matches!(meta.links(), Links::Occupied { .. }) {
            return None;
        }
        Some((Index::from_parts(slot, meta.generation()), value))
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Index<T>, &T)> {
        let mut cursor = self.occupied_head.get();
        core::iter::from_fn(move || {
            let slot = cursor?;
            let Some((meta, value)) = self.entry(slot) else {
                unreachable!("the occupied list points outside the pool");
            };
            let Links::Occupied { next, .. } = meta.links() else {
                unreachable!("the occupied list points at a vacant slot");
            };
            cursor = next;
            Some((Index::from_parts(slot, meta.generation()), value))
        })
    }
}

#[cfg(test)]
mod tests;
