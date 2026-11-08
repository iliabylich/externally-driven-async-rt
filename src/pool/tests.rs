use crate::{
    pool::{Index, ObjectPool},
    test_utils::{aligned_in, assert_yields, stack_memory},
};
use core::{alloc::Layout, cell::Cell, marker::PhantomData, mem::MaybeUninit, ops::Deref};

// A pool in borrowed memory, dropped in place on drop. Only a raw pointer, with the memory's
// provenance: a stored reference would be protected while a `TestPool` is passed to `mem::drop`,
// making the drop UB.
struct TestPool<'m, T> {
    pool: *mut ObjectPool<T>,
    _memory: PhantomData<&'m mut [MaybeUninit<u8>]>,
}

impl<'m, T: Default> TestPool<'m, T> {
    fn new(memory: &'m mut [MaybeUninit<u8>], len: usize) -> Self {
        let ptr = aligned_in(memory, ObjectPool::<T>::layout(len));
        // SAFETY: `ptr` is aligned and valid for the layout, and `memory` stays borrowed for
        // as long as the pool exists.
        unsafe { ObjectPool::<T>::init(ptr, len) };
        Self {
            pool: ObjectPool::raw(ptr, len),
            _memory: PhantomData,
        }
    }
}

impl<T> Deref for TestPool<'_, T> {
    type Target = ObjectPool<T>;

    fn deref(&self) -> &ObjectPool<T> {
        // SAFETY: initialized in `new` and only dropped in `drop`; borrowed through `self`.
        unsafe { &*self.pool }
    }
}

impl<T> Drop for TestPool<'_, T> {
    fn drop(&mut self) {
        // SAFETY: initialized in `new`, dropped once, and not used afterwards.
        unsafe { core::ptr::drop_in_place(self.pool) };
    }
}

fn alloc<T: Default>(pool: &ObjectPool<Cell<T>>, value: T) -> Option<Index<Cell<T>>> {
    let (index, slot) = pool.alloc()?;
    slot.set(value);
    Some(index)
}

fn value<T: Copy>(pool: &ObjectPool<Cell<T>>, index: Index<Cell<T>>) -> Option<T> {
    pool.get(index).map(Cell::get)
}

#[test]
fn layout_matches_the_layout_of_the_value() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u64>>::new(&mut memory, 3);
    assert_eq!(
        Layout::for_value(&*pool),
        ObjectPool::<Cell<u64>>::layout(3)
    );
    assert_eq!(pool.capacity(), 3);
}

#[test]
fn layout_handles_highly_aligned_slots() {
    #[derive(Default)]
    #[repr(align(64))]
    struct Aligned(Cell<u8>);
    let mut memory = stack_memory();
    let pool = TestPool::<Aligned>::new(&mut memory, 3);
    assert_eq!(Layout::for_value(&*pool), ObjectPool::<Aligned>::layout(3));
    let (index, slot) = pool.alloc().unwrap();
    slot.0.set(7);
    let value = pool.get(index).unwrap();
    assert_eq!(value.0.get(), 7);
    assert_eq!((value as *const Aligned).addr() % 64, 0);
}

#[test]
fn alloc_get_free() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<&str>>::new(&mut memory, 2);
    let a = alloc(&pool, "a").unwrap();
    let b = alloc(&pool, "b").unwrap();
    assert_ne!(a, b);
    assert_eq!(value(&pool, a), Some("a"));
    pool.get(b).unwrap().set("B");
    assert_eq!(value(&pool, b), Some("B"));
    pool.free(b);
    assert!(pool.get(b).is_none());
    assert_eq!(value(&pool, a), Some("a"));
}

#[test]
fn a_slot_is_usable_while_another_is_allocated_and_freed() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u32>>::new(&mut memory, 2);
    let (a, slot) = pool.alloc().unwrap();
    let b = alloc(&pool, 2).unwrap();
    pool.free(b);
    slot.set(1);
    assert_eq!(value(&pool, a), Some(1));
}

#[test]
fn index_round_trips_through_bits() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u32>>::new(&mut memory, 2);
    let first = alloc(&pool, 1).unwrap();
    pool.free(first);
    let index = alloc(&pool, 2).unwrap();
    assert_eq!(Index::from_bits(index.to_bits()), index);
    assert_ne!(index.to_bits(), first.to_bits());
    assert!(pool.get(Index::from_bits(first.to_bits())).is_none());
}

#[test]
fn full_pool_rejects_alloc() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u32>>::new(&mut memory, 1);
    alloc(&pool, 1).unwrap();
    assert!(pool.alloc().is_none());
}

#[test]
#[should_panic(expected = "object pool capacity must not be zero")]
fn zero_capacity_is_rejected() {
    ObjectPool::<u32>::layout(0);
}

#[test]
fn freed_slot_is_reused_with_a_new_generation() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u32>>::new(&mut memory, 1);
    let old = alloc(&pool, 1).unwrap();
    pool.free(old);
    let new = alloc(&pool, 2).unwrap();
    assert_ne!(old, new);
    assert!(pool.get(old).is_none());
    pool.free(old);
    assert_eq!(value(&pool, new), Some(2));
}

#[test]
fn double_free_is_rejected() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u32>>::new(&mut memory, 2);
    let index = alloc(&pool, 1).unwrap();
    pool.free(index);
    pool.free(index);
    alloc(&pool, 2).unwrap();
    alloc(&pool, 3).unwrap();
    assert!(pool.alloc().is_none());
}

#[test]
fn every_slot_is_usable_after_mixed_frees() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<usize>>::new(&mut memory, 4);
    let indices = [0, 1, 2, 3].map(|i| alloc(&pool, i).unwrap());
    pool.free(indices[1]);
    pool.free(indices[3]);
    let x = alloc(&pool, 10).unwrap();
    let y = alloc(&pool, 11).unwrap();
    assert!(pool.alloc().is_none());
    assert_eq!(value(&pool, x), Some(10));
    assert_eq!(value(&pool, y), Some(11));
    assert_eq!(value(&pool, indices[0]), Some(0));
    assert_eq!(value(&pool, indices[2]), Some(2));
}

#[test]
fn iter_yields_only_occupied_slots() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u32>>::new(&mut memory, 4);
    let a = alloc(&pool, 1).unwrap();
    let b = alloc(&pool, 2).unwrap();
    let c = alloc(&pool, 3).unwrap();
    pool.free(b);
    assert_yields(
        pool.iter().map(|(index, value)| (index, value.get())),
        [(c, 3), (a, 1)],
    );
}

#[test]
fn freeing_the_yielded_slot_while_iterating() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u32>>::new(&mut memory, 3);
    for value in 0..3 {
        alloc(&pool, value).unwrap();
    }
    let freed = pool.iter().map(|(index, value)| {
        let value = value.get();
        pool.free(index);
        value
    });
    assert_yields(freed, [2, 1, 0]);
    assert!(pool.iter().next().is_none());
}

#[test]
fn occupied_list_stays_consistent() {
    let mut memory = stack_memory();
    let pool = TestPool::<Cell<u32>>::new(&mut memory, 8);
    // What should be in the pool, by slot.
    let mut live: [Option<(Index<Cell<u32>>, u32)>; 8] = [None; 8];
    let mut next_value = 0;
    for step in 0..64u32 {
        let live_count = live.iter().flatten().count();
        if step % 3 == 2 && live_count > 0 {
            let position = (step as usize * 7) % live_count;
            let (index, value) = live.iter().flatten().nth(position).copied().unwrap();
            live[index.slot() as usize] = None;
            assert_eq!(pool.get(index).map(Cell::get), Some(value));
            pool.free(index);
        } else if let Some(index) = alloc(&pool, next_value) {
            live[index.slot() as usize] = Some((index, next_value));
            next_value += 1;
        }
        assert_eq!(pool.iter().count(), live.iter().flatten().count());
        for (index, value) in pool.iter() {
            assert_eq!(live[index.slot() as usize], Some((index, value.get())));
        }
    }
}

#[test]
fn dropping_the_pool_drops_the_values() {
    #[derive(Default)]
    struct Flag<'a>(Cell<Option<&'a Cell<u32>>>);
    impl Drop for Flag<'_> {
        fn drop(&mut self) {
            if let Some(drops) = self.0.get() {
                drops.set(drops.get() + 1);
            }
        }
    }
    let drops = Cell::new(0);
    let mut memory = stack_memory();
    let pool = TestPool::<Flag>::new(&mut memory, 3);
    pool.alloc().unwrap().1.0.set(Some(&drops));
    let (freed, flag) = pool.alloc().unwrap();
    flag.0.set(Some(&drops));
    pool.get(freed).unwrap().0.take();
    pool.free(freed);
    drop(pool);
    assert_eq!(drops.get(), 1);
}
