use alloc::alloc::Layout;
use core::fmt;
use core::marker::PhantomData;
use core::mem;
use core::ptr::{self, NonNull};

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

enum Slot<T> {
    Occupied {
        generation: u32,
        value: T,
        // Links of the doubly-linked list of occupied slots (O(1) unlinking in `free`).
        prev: Option<u32>,
        next: Option<u32>,
    },
    Vacant {
        generation: u32,
        next_free: Option<u32>,
    },
}

// `repr(C)` fixes the field order and offsets, so `layout` can compute the size of a pool with
// `len` slots and `init` can place one in raw memory.
#[repr(C)]
pub(crate) struct ObjectPool<T> {
    free_head: Option<u32>,
    occupied_head: Option<u32>,
    slots: [Slot<T>],
}

impl<T> ObjectPool<T> {
    pub(crate) fn layout(len: usize) -> Layout {
        assert!(len > 0, "object pool capacity must not be zero");
        assert!(
            len <= u32::MAX as usize,
            "object pool capacity must fit in u32"
        );
        let slots = Layout::array::<Slot<T>>(len).expect("object pool is too large");
        let head = Layout::new::<Option<u32>>();
        let (layout, _) = head.extend(head).expect("object pool is too large");
        let (layout, _) = layout.extend(slots).expect("object pool is too large");
        layout.pad_to_align()
    }

    /// Writes an empty pool with `len` slots at `ptr`.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for writes of `Self::layout(len)` bytes (so `layout` must have accepted
    /// `len`), aligned to it, and stay valid (and otherwise unused) for as long as the returned
    /// pool is used.
    pub(crate) unsafe fn init(ptr: NonNull<u8>, len: usize) -> NonNull<Self> {
        let pool = ptr::slice_from_raw_parts_mut(ptr.as_ptr().cast::<Slot<T>>(), len) as *mut Self;
        // SAFETY: the caller guarantees `ptr` is valid for `Self::layout(len)`, which (`repr(C)`)
        // covers every field; every slot is written without reading the uninitialized memory.
        unsafe {
            (&raw mut (*pool).free_head).write(Some(0));
            (&raw mut (*pool).occupied_head).write(None);
            let slots = (&raw mut (*pool).slots).cast::<Slot<T>>();
            for slot in 0..len {
                let next_free = if slot + 1 < len {
                    Some((slot + 1) as u32)
                } else {
                    None
                };
                slots.add(slot).write(Slot::Vacant {
                    generation: 0,
                    next_free,
                });
            }
            NonNull::new_unchecked(pool)
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn alloc(&mut self, value: T) -> Result<Index<T>, T> {
        let Some(slot) = self.free_head else {
            return Err(value);
        };
        let entry = &mut self.slots[slot as usize];
        let Slot::Vacant {
            generation,
            next_free,
        } = *entry
        else {
            unreachable!("the free list points at an occupied slot");
        };
        *entry = Slot::Occupied {
            generation,
            value,
            prev: None,
            next: self.occupied_head,
        };
        self.free_head = next_free;
        if let Some(head) = self.occupied_head {
            self.set_prev(head, Some(slot));
        }
        self.occupied_head = Some(slot);
        Ok(Index {
            slot,
            generation,
            _marker: PhantomData,
        })
    }

    pub(crate) fn free(&mut self, index: Index<T>) -> Option<T> {
        let entry = self.slots.get_mut(index.slot as usize)?;
        match entry {
            Slot::Occupied { generation, .. } if *generation == index.generation => {}
            _ => return None,
        }
        let vacant = Slot::Vacant {
            generation: index.generation.wrapping_add(1),
            next_free: self.free_head,
        };
        let Slot::Occupied {
            value, prev, next, ..
        } = mem::replace(entry, vacant)
        else {
            unreachable!("checked above");
        };
        match prev {
            Some(prev) => self.set_next(prev, next),
            None => self.occupied_head = next,
        }
        if let Some(next) = next {
            self.set_prev(next, prev);
        }
        self.free_head = Some(index.slot);
        Some(value)
    }

    fn set_prev(&mut self, slot: u32, link: Option<u32>) {
        match &mut self.slots[slot as usize] {
            Slot::Occupied { prev, .. } => *prev = link,
            Slot::Vacant { .. } => unreachable!("the occupied list points at a vacant slot"),
        }
    }

    fn set_next(&mut self, slot: u32, link: Option<u32>) {
        match &mut self.slots[slot as usize] {
            Slot::Occupied { next, .. } => *next = link,
            Slot::Vacant { .. } => unreachable!("the occupied list points at a vacant slot"),
        }
    }

    pub(crate) fn get(&self, index: Index<T>) -> Option<&T> {
        match self.slots.get(index.slot as usize)? {
            Slot::Occupied {
                generation, value, ..
            } if *generation == index.generation => Some(value),
            _ => None,
        }
    }

    pub(crate) fn get_mut(&mut self, index: Index<T>) -> Option<&mut T> {
        match self.slots.get_mut(index.slot as usize)? {
            Slot::Occupied {
                generation, value, ..
            } if *generation == index.generation => Some(value),
            _ => None,
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Index<T>, &T)> {
        let mut cursor = self.occupied_head;
        core::iter::from_fn(move || {
            let slot = cursor?;
            let Slot::Occupied {
                generation,
                value,
                next,
                ..
            } = &self.slots[slot as usize]
            else {
                unreachable!("the occupied list points at a vacant slot");
            };
            cursor = *next;
            Some((
                Index {
                    slot,
                    generation: *generation,
                    _marker: PhantomData,
                },
                value,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;
    use std::rc::Rc;

    fn boxed<T>(len: usize) -> Box<ObjectPool<T>> {
        let layout = ObjectPool::<T>::layout(len);
        // SAFETY: `layout` isn't zero-sized (it always holds `free_head`).
        let ptr = NonNull::new(unsafe { alloc::alloc::alloc(layout) }).unwrap();
        // SAFETY: `ptr` is a fresh allocation of `layout`. `Box` frees it with the layout of the
        // value, which equals `layout` (checked by Miri).
        unsafe { Box::from_raw(ObjectPool::<T>::init(ptr, len).as_ptr()) }
    }

    #[test]
    fn layout_matches_the_layout_of_the_value() {
        let pool = boxed::<u64>(3);
        assert_eq!(Layout::for_value(&*pool), ObjectPool::<u64>::layout(3));
        assert_eq!(pool.capacity(), 3);
    }

    #[test]
    fn layout_handles_highly_aligned_slots() {
        #[repr(align(64))]
        struct Aligned(u8);
        let mut pool = boxed::<Aligned>(3);
        assert_eq!(Layout::for_value(&*pool), ObjectPool::<Aligned>::layout(3));
        let index = pool.alloc(Aligned(7)).ok().unwrap();
        let value = pool.get(index).unwrap();
        assert_eq!(value.0, 7);
        assert_eq!((value as *const Aligned).addr() % 64, 0);
    }

    #[test]
    fn alloc_get_free() {
        let mut pool = boxed::<&str>(2);
        let a = pool.alloc("a").unwrap();
        let b = pool.alloc("b").unwrap();
        assert_ne!(a, b);
        assert_eq!(pool.get(a), Some(&"a"));
        *pool.get_mut(b).unwrap() = "B";
        assert_eq!(pool.free(b), Some("B"));
        assert_eq!(pool.free(a), Some("a"));
    }

    #[test]
    fn index_round_trips_through_bits() {
        let mut pool = boxed::<u32>(2);
        let first = pool.alloc(1).unwrap();
        pool.free(first);
        let index = pool.alloc(2).unwrap();
        assert_eq!(Index::from_bits(index.to_bits()), index);
        assert_ne!(index.to_bits(), first.to_bits());
        assert_eq!(pool.get(Index::from_bits(first.to_bits())), None);
    }

    #[test]
    fn full_pool_returns_the_value() {
        let mut pool = boxed::<u32>(1);
        pool.alloc(1).unwrap();
        assert_eq!(pool.alloc(2), Err(2));
    }

    #[test]
    #[should_panic(expected = "object pool capacity must not be zero")]
    fn zero_capacity_is_rejected() {
        ObjectPool::<u32>::layout(0);
    }

    #[test]
    fn freed_slot_is_reused_with_a_new_generation() {
        let mut pool = boxed::<u32>(1);
        let old = pool.alloc(1).unwrap();
        pool.free(old);
        let new = pool.alloc(2).unwrap();
        assert_ne!(old, new);
        assert_eq!(pool.get(old), None);
        assert_eq!(pool.get_mut(old), None);
        assert_eq!(pool.free(old), None);
        assert_eq!(pool.get(new), Some(&2));
    }

    #[test]
    fn double_free_is_rejected() {
        let mut pool = boxed::<u32>(2);
        let index = pool.alloc(1).unwrap();
        assert_eq!(pool.free(index), Some(1));
        assert_eq!(pool.free(index), None);
        pool.alloc(2).unwrap();
        pool.alloc(3).unwrap();
        assert_eq!(pool.alloc(4), Err(4));
    }

    #[test]
    fn every_slot_is_usable_after_mixed_frees() {
        let mut pool = boxed::<usize>(4);
        let indices: Vec<_> = (0..4).map(|i| pool.alloc(i).unwrap()).collect();
        pool.free(indices[1]);
        pool.free(indices[3]);
        let x = pool.alloc(10).unwrap();
        let y = pool.alloc(11).unwrap();
        assert_eq!(pool.alloc(12), Err(12));
        assert_eq!(pool.get(x), Some(&10));
        assert_eq!(pool.get(y), Some(&11));
        assert_eq!(pool.get(indices[0]), Some(&0));
        assert_eq!(pool.get(indices[2]), Some(&2));
    }

    #[test]
    fn iter_yields_only_occupied_slots() {
        let mut pool = boxed::<u32>(4);
        let a = pool.alloc(1).unwrap();
        let b = pool.alloc(2).unwrap();
        let c = pool.alloc(3).unwrap();
        pool.free(b);
        let items: Vec<_> = pool.iter().map(|(index, value)| (index, *value)).collect();
        assert_eq!(items, vec![(c, 3), (a, 1)]);
    }

    #[test]
    fn occupied_list_stays_consistent() {
        let mut pool = boxed::<u32>(8);
        let mut live: Vec<(Index<u32>, u32)> = Vec::new();
        let mut next_value = 0;
        for step in 0..64u32 {
            if step % 3 == 2 && !live.is_empty() {
                let position = (step as usize * 7) % live.len();
                let (index, value) = live.remove(position);
                assert_eq!(pool.free(index), Some(value));
            } else if let Ok(index) = pool.alloc(next_value) {
                live.push((index, next_value));
                next_value += 1;
            }
            let mut actual: Vec<_> = pool.iter().map(|(index, value)| (index, *value)).collect();
            let mut expected = live.clone();
            actual.sort_by_key(|(_, value)| *value);
            expected.sort_by_key(|(_, value)| *value);
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn dropping_the_pool_drops_the_objects() {
        struct Flag(Rc<Cell<u32>>);
        impl Drop for Flag {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }
        let drops = Rc::new(Cell::new(0));
        let mut pool = boxed::<Flag>(3);
        pool.alloc(Flag(drops.clone())).ok().unwrap();
        let freed = pool.alloc(Flag(drops.clone())).ok().unwrap();
        drop(pool.free(freed));
        assert_eq!(drops.get(), 1);
        drop(pool);
        assert_eq!(drops.get(), 2);
    }
}
