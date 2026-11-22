use super::FdHashMap;
use core::cell::Cell;

fn insert<const N: usize>(map: &FdHashMap<Cell<i32>, N>, key: i32, value: i32) -> Option<()> {
    map.get_or_insert(key)?.set(value);
    Some(())
}

fn value<const N: usize>(map: &FdHashMap<Cell<i32>, N>, key: i32) -> Option<i32> {
    map.get(key).map(Cell::get)
}

#[test]
fn distinct_keys_get_distinct_values() {
    let map = &FdHashMap::<Cell<i32>, 2>::new([const { Cell::new(0) }; 2]);
    insert(map, 1, 10).unwrap();
    insert(map, 2, 20).unwrap();
    assert_eq!(value(map, 1), Some(10));
    assert_eq!(value(map, 2), Some(20));
    assert_eq!(value(map, 3), None);
}

#[test]
fn get_or_insert_returns_the_existing_value() {
    let map = &FdHashMap::<Cell<i32>, 2>::new([const { Cell::new(0) }; 2]);
    insert(map, 1, 10).unwrap();
    assert_eq!(map.get_or_insert(1).map(Cell::get), Some(10));
    assert_eq!(map.iter().count(), 1);
}

#[test]
fn a_full_map_rejects_new_keys_but_finds_existing_ones() {
    let map = &FdHashMap::<Cell<i32>, 2>::new([const { Cell::new(0) }; 2]);
    insert(map, 1, 10).unwrap();
    insert(map, 2, 20).unwrap();
    assert!(map.get_or_insert(3).is_none());
    assert_eq!(map.get_or_insert(1).map(Cell::get), Some(10));
}

#[test]
fn removing_a_key_frees_its_slot() {
    let map = &FdHashMap::<Cell<i32>, 1>::new([const { Cell::new(0) }; 1]);
    insert(map, 1, 10).unwrap();
    map.remove(1);
    assert_eq!(value(map, 1), None);
    insert(map, 2, 20).unwrap();
    assert_eq!(value(map, 2), Some(20));
}

#[test]
fn iter_yields_every_key_with_its_value() {
    let map = &FdHashMap::<Cell<i32>, 4>::new([const { Cell::new(0) }; 4]);
    insert(map, 1, 10).unwrap();
    insert(map, 2, 20).unwrap();
    insert(map, 3, 30).unwrap();
    map.remove(2);
    let mut seen = [None; 4];
    for (slot, (key, value)) in seen.iter_mut().zip(map.iter()) {
        *slot = Some((key, value.get()));
    }
    assert_eq!(seen, [Some((3, 30)), Some((1, 10)), None, None]);
}
