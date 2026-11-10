mod index;
mod links;
mod meta;

use core::cell::Cell;

pub(crate) use self::index::Index;
use self::{links::Links, meta::Meta};

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
            assert!(
                N <= u32::MAX as usize,
                "object pool capacity must fit in u32"
            );
        }
        let mut meta = [const { Meta::new(None) }; N];
        let mut slot = 0;
        while slot + 1 < N {
            meta[slot] = Meta::new(Some((slot + 1) as u32));
            slot += 1;
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
        let meta = &self.meta[slot as usize];
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
        Some((
            Index::from_parts(slot, meta.generation()),
            &self.values[slot as usize],
        ))
    }

    pub(crate) fn free(&self, index: Index<T>) {
        let Some(meta) = self.occupied(index) else {
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

    fn occupied(&self, index: Index<T>) -> Option<&Meta> {
        let meta = self.meta.get(index.slot() as usize)?;
        let occupied = matches!(meta.links(), Links::Occupied { .. });
        if occupied && meta.generation() == index.generation() {
            Some(meta)
        } else {
            None
        }
    }

    fn set_prev(&self, slot: u32, link: Option<u32>) {
        let meta = &self.meta[slot as usize];
        let Links::Occupied { next, .. } = meta.links() else {
            unreachable!("the occupied list points at a vacant slot");
        };
        meta.set_links(Links::Occupied { prev: link, next });
    }

    fn set_next(&self, slot: u32, link: Option<u32>) {
        let meta = &self.meta[slot as usize];
        let Links::Occupied { prev, .. } = meta.links() else {
            unreachable!("the occupied list points at a vacant slot");
        };
        meta.set_links(Links::Occupied { prev, next: link });
    }

    pub(crate) fn get(&self, index: Index<T>) -> Option<&T> {
        self.occupied(index)
            .map(|_| &self.values[index.slot() as usize])
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Index<T>, &T)> {
        let mut cursor = self.occupied_head.get();
        core::iter::from_fn(move || {
            let slot = cursor?;
            let meta = &self.meta[slot as usize];
            let Links::Occupied { next, .. } = meta.links() else {
                unreachable!("the occupied list points at a vacant slot");
            };
            cursor = next;
            Some((
                Index::from_parts(slot, meta.generation()),
                &self.values[slot as usize],
            ))
        })
    }
}

#[cfg(test)]
mod tests;
