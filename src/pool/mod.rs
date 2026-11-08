use core::{cell::Cell, fmt, marker::PhantomData, mem::MaybeUninit};

pub(crate) struct Index<T> {
    slot: u32,
    generation: u32,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Index<T> {
    pub(crate) fn from_parts(slot: u32, generation: u32) -> Self {
        Self {
            slot,
            generation,
            _marker: PhantomData,
        }
    }

    pub(crate) fn slot(self) -> u32 {
        self.slot
    }

    pub(crate) fn generation(self) -> u32 {
        self.generation
    }

    pub(crate) fn to_bits(self) -> u64 {
        (u64::from(self.generation) << 32) | u64::from(self.slot)
    }

    pub(crate) fn from_bits(bits: u64) -> Self {
        Self {
            slot: bits as u32,
            generation: (bits >> 32) as u32,
            _marker: PhantomData,
        }
    }
}

impl<T> Clone for Index<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Index<T> {}

impl<T> PartialEq for Index<T> {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot && self.generation == other.generation
    }
}

impl<T> Eq for Index<T> {}

impl<T> fmt::Debug for Index<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Index({}v{})", self.slot, self.generation)
    }
}

#[derive(Clone, Copy)]
enum Links {
    Occupied {
        prev: Option<u32>,
        next: Option<u32>,
    },
    Vacant {
        next_free: Option<u32>,
    },
}

struct Slot<T> {
    generation: Cell<u32>,
    links: Cell<Links>,
    value: T,
}

pub(crate) struct ObjectPool<T: Default, const N: usize> {
    free_head: Cell<Option<u32>>,
    occupied_head: Cell<Option<u32>>,
    slots: [Slot<T>; N],
}

impl<T: Default, const N: usize> ObjectPool<T, N> {
    pub(crate) fn init(place: &mut MaybeUninit<Self>) -> &Self {
        const {
            assert!(N > 0, "object pool capacity must not be zero");
            assert!(
                N <= u32::MAX as usize,
                "object pool capacity must fit in u32"
            );
        }

        let ptr = place.as_mut_ptr();
        // SAFETY: `ptr` points to `place`, borrowed exclusively until the end; the fields are
        // disjoint, `&raw mut` creates no reference to them while they're uninitialized, and
        // `MaybeUninit<T>` has `T`'s layout.
        let (free_head, occupied_head, slots) = unsafe {
            (
                &mut *(&raw mut (*ptr).free_head).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).occupied_head).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).slots).cast::<[MaybeUninit<_>; N]>(),
            )
        };
        free_head.write(Cell::new(Some(0u32)));
        occupied_head.write(Cell::new(None::<u32>));
        for (slot, place) in slots.iter_mut().enumerate() {
            let next_free = if slot + 1 < N {
                Some((slot + 1) as u32)
            } else {
                None
            };
            place.write(Slot {
                generation: Cell::new(0),
                links: Cell::new(Links::Vacant { next_free }),
                value: T::default(),
            });
        }
        // SAFETY: every field was written above.
        unsafe { place.assume_init_ref() }
    }

    pub(crate) fn alloc(&self) -> Option<(Index<T>, &T)> {
        let slot = self.free_head.get()?;
        let entry = &self.slots[slot as usize];
        let Links::Vacant { next_free } = entry.links.get() else {
            unreachable!("the free list points at an occupied slot");
        };
        let head = self.occupied_head.get();
        entry.links.set(Links::Occupied {
            prev: None,
            next: head,
        });
        self.free_head.set(next_free);
        if let Some(head) = head {
            self.set_prev(head, Some(slot));
        }
        self.occupied_head.set(Some(slot));
        Some((
            Index::from_parts(slot, entry.generation.get()),
            &entry.value,
        ))
    }

    pub(crate) fn free(&self, index: Index<T>) {
        let Some(entry) = self.occupied(index) else {
            return;
        };
        let Links::Occupied { prev, next } = entry.links.get() else {
            unreachable!("checked by `occupied`");
        };
        entry.generation.set(index.generation.wrapping_add(1));
        entry.links.set(Links::Vacant {
            next_free: self.free_head.get(),
        });
        match prev {
            Some(prev) => self.set_next(prev, next),
            None => self.occupied_head.set(next),
        }
        if let Some(next) = next {
            self.set_prev(next, prev);
        }
        self.free_head.set(Some(index.slot));
    }

    fn occupied(&self, index: Index<T>) -> Option<&Slot<T>> {
        let entry = self.slots.get(index.slot as usize)?;
        let occupied = matches!(entry.links.get(), Links::Occupied { .. });
        if occupied && entry.generation.get() == index.generation {
            Some(entry)
        } else {
            None
        }
    }

    fn set_prev(&self, slot: u32, link: Option<u32>) {
        let entry = &self.slots[slot as usize];
        let Links::Occupied { next, .. } = entry.links.get() else {
            unreachable!("the occupied list points at a vacant slot");
        };
        entry.links.set(Links::Occupied { prev: link, next });
    }

    fn set_next(&self, slot: u32, link: Option<u32>) {
        let entry = &self.slots[slot as usize];
        let Links::Occupied { prev, .. } = entry.links.get() else {
            unreachable!("the occupied list points at a vacant slot");
        };
        entry.links.set(Links::Occupied { prev, next: link });
    }

    pub(crate) fn get(&self, index: Index<T>) -> Option<&T> {
        self.occupied(index).map(|entry| &entry.value)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Index<T>, &T)> {
        let mut cursor = self.occupied_head.get();
        core::iter::from_fn(move || {
            let slot = cursor?;
            let entry = &self.slots[slot as usize];
            let Links::Occupied { next, .. } = entry.links.get() else {
                unreachable!("the occupied list points at a vacant slot");
            };
            cursor = next;
            Some((
                Index::from_parts(slot, entry.generation.get()),
                &entry.value,
            ))
        })
    }
}

#[cfg(test)]
mod tests;
