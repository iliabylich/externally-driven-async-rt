use core::{alloc::Layout, cell::Cell, fmt, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

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
    // The doubly-linked list of occupied slots (O(1) unlinking in `free`).
    Occupied {
        prev: Option<u32>,
        next: Option<u32>,
    },
    Vacant {
        next_free: Option<u32>,
    },
}

// The value stays in place for the pool's whole life (a vacant slot holds a stale or default
// one), so the pool can hand out `&T` while every method takes `&self`; `T` is made of `Cell`s.
struct Slot<T> {
    generation: Cell<u32>,
    links: Cell<Links>,
    value: T,
}

// `repr(C)` fixes the field order and offsets, so `layout` can compute the size of a pool with
// `len` slots and `init` can place one in raw memory.
#[repr(C)]
pub(crate) struct ObjectPool<T> {
    free_head: Cell<Option<u32>>,
    occupied_head: Cell<Option<u32>>,
    slots: [Slot<T>],
}

impl<T: Default> ObjectPool<T> {
    /// Writes an empty pool with `len` slots at `ptr`, each holding `T::default()`.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for writes of `Self::layout(len)` bytes (so `layout` must have accepted
    /// `len`), aligned to it, and stay valid (and otherwise unused) for `'a`.
    pub(crate) unsafe fn init<'a>(ptr: NonNull<u8>, len: usize) -> &'a Self {
        let pool = Self::raw(ptr, len);
        // SAFETY: the caller guarantees `ptr` is valid for `Self::layout(len)`, which (`repr(C)`)
        // covers every field, disjointly. `&raw mut` creates no reference to the uninitialized
        // pool, and the fields are borrowed as `MaybeUninit`, which needs no initialized contents.
        let (free_head, occupied_head, slots) = unsafe {
            (
                &mut *(&raw mut (*pool).free_head).cast::<MaybeUninit<Cell<Option<u32>>>>(),
                &mut *(&raw mut (*pool).occupied_head).cast::<MaybeUninit<Cell<Option<u32>>>>(),
                &mut *(&raw mut (*pool).slots as *mut [MaybeUninit<Slot<T>>]),
            )
        };
        free_head.write(Cell::new(Some(0)));
        occupied_head.write(Cell::new(None));
        for (slot, place) in slots.iter_mut().enumerate() {
            let next_free = if slot + 1 < len {
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
        // SAFETY: every field was written above, and the memory stays valid for `'a` (guaranteed
        // by the caller).
        unsafe { &*pool }
    }
}

impl<T> ObjectPool<T> {
    pub(crate) fn layout(len: usize) -> Layout {
        assert!(len > 0, "object pool capacity must not be zero");
        assert!(
            len <= u32::MAX as usize,
            "object pool capacity must fit in u32"
        );
        let slots = Layout::array::<Slot<T>>(len).expect("object pool is too large");
        let head = Layout::new::<Cell<Option<u32>>>();
        let (layout, _) = head.extend(head).expect("object pool is too large");
        let (layout, _) = layout.extend(slots).expect("object pool is too large");
        layout.pad_to_align()
    }

    /// The pool of `len` slots at `ptr`, with `ptr`'s provenance.
    pub(crate) fn raw(ptr: NonNull<u8>, len: usize) -> *mut Self {
        core::ptr::slice_from_raw_parts_mut(ptr.as_ptr().cast::<Slot<T>>(), len) as *mut Self
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Takes a free slot, whose value is left as it was: the caller sets it.
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

    /// Its value stays in place until the slot is reused, so the caller takes out what must not
    /// linger (wakers, futures) through `get` first. A stale `index` is ignored.
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

    /// Freeing the yielded slot while iterating is fine; freeing any other one is not.
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
