use super::{Entry, FdTree};
use rustix::fd::RawFd;

fn insert<const N: usize>(tree: &FdTree<N>, key: RawFd, idx: u32) {
    let Entry::Vacant(vacant) = tree.entry(key) else {
        panic!("{key} is already in the tree");
    };
    vacant.insert(idx);
}

#[test]
fn an_empty_tree_has_no_keys() {
    let tree = &FdTree::<2>::new();
    assert_eq!(tree.find(0), None);
    assert_eq!(tree.key(0), None);
    assert!(matches!(tree.entry(0), Entry::Vacant(_)));
}

#[test]
fn an_inserted_key_is_found_by_key_and_by_index() {
    let tree = &FdTree::<2>::new();
    insert(tree, 3, 1);
    insert(tree, -1, 0);
    assert_eq!(tree.find(3), Some(1));
    assert_eq!(tree.find(-1), Some(0));
    assert_eq!(tree.find(4), None);
    assert_eq!(tree.key(1), Some(3));
    assert_eq!(tree.key(0), Some(-1));
}

#[test]
fn an_inserted_key_is_occupied() {
    let tree = &FdTree::<2>::new();
    insert(tree, 7, 1);
    assert!(matches!(tree.entry(7), Entry::Occupied(1)));
}

#[test]
fn removing_returns_the_index_and_forgets_the_key() {
    let tree = &FdTree::<2>::new();
    insert(tree, 7, 1);
    assert_eq!(tree.remove(7), 1);
    assert_eq!(tree.find(7), None);
    assert_eq!(tree.key(1), None);
    assert!(matches!(tree.entry(7), Entry::Vacant(_)));
}

#[test]
fn removing_the_root_keeps_its_descendants_findable() {
    let tree = &FdTree::<3>::new();
    insert(tree, 10, 0);
    insert(tree, 20, 1);
    insert(tree, 30, 2);
    assert_eq!(tree.remove(10), 0);
    assert_eq!(tree.find(20), Some(1));
    assert_eq!(tree.find(30), Some(2));
}

#[test]
fn a_removed_index_can_hold_another_key() {
    let tree = &FdTree::<2>::new();
    insert(tree, 1, 0);
    insert(tree, 2, 1);
    tree.remove(1);
    insert(tree, 5, 0);
    assert_eq!(tree.find(1), None);
    assert_eq!(tree.find(5), Some(0));
    assert_eq!(tree.key(0), Some(5));
    assert_eq!(tree.find(2), Some(1));
}

#[test]
fn extreme_keys_are_distinct() {
    let tree = &FdTree::<4>::new();
    let keys = [RawFd::MIN, -1, 0, RawFd::MAX];
    for (idx, key) in (0..).zip(keys) {
        insert(tree, key, idx);
    }
    for (idx, key) in (0..).zip(keys) {
        assert_eq!(tree.find(key), Some(idx));
        assert_eq!(tree.key(idx), Some(key));
    }
}

#[test]
fn removing_in_any_order_keeps_the_rest_findable() {
    let tree = &FdTree::<16>::new();
    let keys: [RawFd; 16] = core::array::from_fn(|i| RawFd::try_from(i).unwrap() * 7 - 50);
    for (idx, key) in (0..).zip(keys) {
        insert(tree, key, idx);
    }
    let order = [9, 0, 15, 4, 11, 2, 7, 13, 1, 14, 3, 8, 12, 5, 10, 6];
    let mut removed = [false; 16];
    for i in order {
        assert_eq!(tree.remove(keys[i]), u32::try_from(i).unwrap());
        removed[i] = true;
        for (idx, (key, removed)) in (0..).zip(keys.iter().zip(removed)) {
            let expected = if removed { None } else { Some(idx) };
            assert_eq!(tree.find(*key), expected);
        }
    }
}
