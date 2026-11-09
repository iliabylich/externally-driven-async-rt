use crate::{
    pool::{Index, ObjectPool},
    testing::assert_yields,
};
use core::cell::Cell;

fn alloc<T, const N: usize>(pool: &ObjectPool<Cell<T>, N>, value: T) -> Option<Index<Cell<T>>> {
    let (index, slot) = pool.alloc()?;
    slot.set(value);
    Some(index)
}

fn value<T: Copy, const N: usize>(
    pool: &ObjectPool<Cell<T>, N>,
    index: Index<Cell<T>>,
) -> Option<T> {
    pool.get(index).map(Cell::get)
}

#[test]
fn highly_aligned_slots_are_aligned() {
    #[repr(align(64))]
    struct Aligned(Cell<u8>);
    let pool = &ObjectPool::<Aligned, 3>::new([const { Aligned(Cell::new(0)) }; 3]);
    let (index, slot) = pool.alloc().unwrap();
    slot.0.set(7);
    let value = pool.get(index).unwrap();
    assert_eq!(value.0.get(), 7);
    assert_eq!((value as *const Aligned).addr() % 64, 0);
}

#[test]
fn alloc_get_free() {
    let pool = &ObjectPool::<Cell<&str>, 2>::new([const { Cell::new("") }; 2]);
    let a = alloc(pool, "a").unwrap();
    let b = alloc(pool, "b").unwrap();
    assert_ne!(a, b);
    assert_eq!(value(pool, a), Some("a"));
    pool.get(b).unwrap().set("B");
    assert_eq!(value(pool, b), Some("B"));
    pool.free(b);
    assert!(pool.get(b).is_none());
    assert_eq!(value(pool, a), Some("a"));
}

#[test]
fn a_slot_is_usable_while_another_is_allocated_and_freed() {
    let pool = &ObjectPool::<Cell<u32>, 2>::new([const { Cell::new(0) }; 2]);
    let (a, slot) = pool.alloc().unwrap();
    let b = alloc(pool, 2).unwrap();
    pool.free(b);
    slot.set(1);
    assert_eq!(value(pool, a), Some(1));
}

#[test]
fn index_round_trips_through_bits() {
    let pool = &ObjectPool::<Cell<u32>, 2>::new([const { Cell::new(0) }; 2]);
    let first = alloc(pool, 1).unwrap();
    pool.free(first);
    let index = alloc(pool, 2).unwrap();
    assert_eq!(Index::from_bits(index.to_bits()), index);
    assert_ne!(index.to_bits(), first.to_bits());
    assert!(pool.get(Index::from_bits(first.to_bits())).is_none());
}

#[test]
fn full_pool_rejects_alloc() {
    let pool = &ObjectPool::<Cell<u32>, 1>::new([const { Cell::new(0) }; 1]);
    alloc(pool, 1).unwrap();
    assert!(pool.alloc().is_none());
}

#[test]
fn freed_slot_is_reused_with_a_new_generation() {
    let pool = &ObjectPool::<Cell<u32>, 1>::new([const { Cell::new(0) }; 1]);
    let old = alloc(pool, 1).unwrap();
    pool.free(old);
    let new = alloc(pool, 2).unwrap();
    assert_ne!(old, new);
    assert!(pool.get(old).is_none());
    pool.free(old);
    assert_eq!(value(pool, new), Some(2));
}

#[test]
fn double_free_is_rejected() {
    let pool = &ObjectPool::<Cell<u32>, 2>::new([const { Cell::new(0) }; 2]);
    let index = alloc(pool, 1).unwrap();
    pool.free(index);
    pool.free(index);
    alloc(pool, 2).unwrap();
    alloc(pool, 3).unwrap();
    assert!(pool.alloc().is_none());
}

#[test]
fn every_slot_is_usable_after_mixed_frees() {
    let pool = &ObjectPool::<Cell<usize>, 4>::new([const { Cell::new(0) }; 4]);
    let indices = [0, 1, 2, 3].map(|i| alloc(pool, i).unwrap());
    pool.free(indices[1]);
    pool.free(indices[3]);
    let x = alloc(pool, 10).unwrap();
    let y = alloc(pool, 11).unwrap();
    assert!(pool.alloc().is_none());
    assert_eq!(value(pool, x), Some(10));
    assert_eq!(value(pool, y), Some(11));
    assert_eq!(value(pool, indices[0]), Some(0));
    assert_eq!(value(pool, indices[2]), Some(2));
}

#[test]
fn iter_yields_only_occupied_slots() {
    let pool = &ObjectPool::<Cell<u32>, 4>::new([const { Cell::new(0) }; 4]);
    let a = alloc(pool, 1).unwrap();
    let b = alloc(pool, 2).unwrap();
    let c = alloc(pool, 3).unwrap();
    pool.free(b);
    assert_yields!(
        pool.iter().map(|(index, value)| (index, value.get())),
        [(c, 3), (a, 1)],
    );
}

#[test]
fn freeing_the_yielded_slot_while_iterating() {
    let pool = &ObjectPool::<Cell<u32>, 3>::new([const { Cell::new(0) }; 3]);
    for value in 0..3 {
        alloc(pool, value).unwrap();
    }
    let freed = pool.iter().map(|(index, value)| {
        let value = value.get();
        pool.free(index);
        value
    });
    assert_yields!(freed, [2, 1, 0]);
    assert!(pool.iter().next().is_none());
}

#[test]
fn occupied_list_stays_consistent() {
    let pool = &ObjectPool::<Cell<u32>, 8>::new([const { Cell::new(0) }; 8]);
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
        } else if let Some(index) = alloc(pool, next_value) {
            live[index.slot() as usize] = Some((index, next_value));
            next_value += 1;
        }
        assert_eq!(pool.iter().count(), live.iter().flatten().count());
        for (index, value) in pool.iter() {
            assert_eq!(live[index.slot() as usize], Some((index, value.get())));
        }
    }
}
