use core::cell::Cell;

use crate::hash_map::HashMap;

fn insert<const N: usize>(map: &HashMap<i32, Cell<i32>, N>, key: i32) -> Option<()> {
    map.get_or_insert(key)?.set(key);
    Some(())
}

fn value<const N: usize>(map: &HashMap<i32, Cell<i32>, N>, key: i32) -> Option<i32> {
    map.get(key).map(Cell::get)
}

#[test]
fn an_inserted_key_is_found_again() {
    let map = &HashMap::<i32, Cell<i32>, 2>::new([const { Cell::new(0) }; 2]);
    insert(map, 7).unwrap();
    assert_eq!(value(map, 7), Some(7));
    assert_eq!(value(map, 8), None);
}

#[test]
fn distinct_keys_get_distinct_entries() {
    let map = &HashMap::<i32, Cell<i32>, 2>::new([const { Cell::new(0) }; 2]);
    insert(map, 1).unwrap();
    insert(map, 2).unwrap();
    assert_eq!(value(map, 1), Some(1));
    assert_eq!(value(map, 2), Some(2));
}

#[test]
fn get_or_insert_returns_the_existing_entry() {
    let map = &HashMap::<i32, Cell<i32>, 2>::new([const { Cell::new(0) }; 2]);
    insert(map, 1).unwrap();
    assert_eq!(map.get_or_insert(1).map(Cell::get), Some(1));
    assert_eq!(map.iter().count(), 1);
}

#[test]
fn a_full_map_rejects_new_keys_but_finds_existing_ones() {
    let map = &HashMap::<i32, Cell<i32>, 2>::new([const { Cell::new(0) }; 2]);
    insert(map, 1).unwrap();
    insert(map, 2).unwrap();
    assert!(map.get_or_insert(3).is_none());
    assert_eq!(map.get_or_insert(1).map(Cell::get), Some(1));
}

#[test]
fn a_removed_key_is_gone() {
    let map = &HashMap::<i32, Cell<i32>, 1>::new([const { Cell::new(0) }; 1]);
    insert(map, 1).unwrap();
    map.remove(1);
    assert_eq!(value(map, 1), None);
    assert_eq!(map.iter().count(), 0);
}

#[test]
fn removing_any_node_keeps_the_others_findable() {
    let map = &HashMap::<i32, Cell<i32>, 32>::new([const { Cell::new(0) }; 32]);
    let keys: [i32; 32] = core::array::from_fn(|i| i32::try_from(i).unwrap());
    for key in keys {
        insert(map, key).unwrap();
    }
    let removed = [0, 31, 5, 17, 1, 30, 12, 8, 23, 2];
    for (step, key) in removed.iter().enumerate() {
        map.remove(*key);
        for key in keys {
            let expected = (!removed[..=step].contains(&key)).then_some(key);
            assert_eq!(value(map, key), expected);
        }
    }
    assert_eq!(map.iter().count(), 22);
}

#[test]
fn removed_keys_can_be_inserted_again() {
    let map = &HashMap::<i32, Cell<i32>, 16>::new([const { Cell::new(0) }; 16]);
    let keys: [i32; 16] = core::array::from_fn(|i| i32::try_from(i * 3).unwrap());
    for key in keys {
        insert(map, key).unwrap();
    }
    for key in keys.iter().step_by(2) {
        map.remove(*key);
    }
    for key in keys.iter().step_by(2) {
        insert(map, *key).unwrap();
    }
    for key in keys {
        assert_eq!(value(map, key), Some(key));
    }
    assert_eq!(map.iter().count(), 16);
}

#[test]
fn iter_yields_every_entry() {
    let map = &HashMap::<i32, Cell<i32>, 4>::new([const { Cell::new(0) }; 4]);
    insert(map, 10).unwrap();
    insert(map, 20).unwrap();
    insert(map, 30).unwrap();
    map.remove(20);
    let mut seen = [None; 4];
    for (slot, (key, value)) in seen.iter_mut().zip(map.iter()) {
        *slot = Some((key, value.get()));
    }
    assert_eq!(seen, [Some((30, 30)), Some((10, 10)), None, None]);
}

#[test]
fn keys_with_equal_hashes_chain_and_stay_removable() {
    #[derive(Clone, Copy, PartialEq, Eq)]
    struct Colliding(u8);

    impl core::hash::Hash for Colliding {
        fn hash<H: core::hash::Hasher>(&self, _: &mut H) {}
    }

    let map = &HashMap::<Colliding, Cell<u8>, 4>::new([const { Cell::new(0) }; 4]);
    for key in 0..4 {
        map.get_or_insert(Colliding(key)).unwrap().set(key);
    }
    map.remove(Colliding(1));
    map.remove(Colliding(0));
    assert!(map.get(Colliding(0)).is_none());
    assert!(map.get(Colliding(1)).is_none());
    assert_eq!(map.get(Colliding(2)).map(Cell::get), Some(2));
    assert_eq!(map.get(Colliding(3)).map(Cell::get), Some(3));
    assert_eq!(map.iter().count(), 2);
}
