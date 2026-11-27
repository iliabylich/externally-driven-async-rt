use super::IndexedMinHeap;
use crate::testing::assert_yields;

fn drain<const N: usize>(heap: &IndexedMinHeap<u32, N>) -> impl Iterator<Item = (u32, u32)> {
    core::iter::from_fn(move || {
        let (idx, key) = heap.peek()?;
        heap.remove(idx);
        Some((idx, key))
    })
}

#[test]
fn an_empty_heap_has_no_minimum() {
    let heap = IndexedMinHeap::<u32, 2>::new();
    assert_eq!(heap.peek(), None);
    assert!(!heap.contains(0));
}

#[test]
fn peek_returns_the_minimum_regardless_of_insertion_order() {
    let heap = IndexedMinHeap::<u32, 4>::new();
    heap.insert(0, 30);
    heap.insert(1, 10);
    heap.insert(2, 40);
    heap.insert(3, 20);
    assert_eq!(heap.peek(), Some((1, 10)));
    assert_yields!(drain(&heap), [(1, 10), (3, 20), (0, 30), (2, 40)]);
    assert_eq!(heap.peek(), None);
}

#[test]
fn contains_follows_insert_and_remove() {
    let heap = IndexedMinHeap::<u32, 2>::new();
    heap.insert(1, 5);
    assert!(heap.contains(1));
    assert!(!heap.contains(0));
    heap.remove(1);
    assert!(!heap.contains(1));
}

#[test]
fn removing_any_slot_keeps_the_order_of_the_rest() {
    let heap = IndexedMinHeap::<u32, 6>::new();
    for (idx, key) in [(0, 50), (1, 10), (2, 40), (3, 20), (4, 60), (5, 30)] {
        heap.insert(idx, key);
    }
    heap.remove(1);
    heap.remove(2);
    heap.remove(4);
    assert_yields!(drain(&heap), [(3, 20), (5, 30), (0, 50)]);
}

#[test]
fn a_removed_slot_can_be_inserted_with_another_key() {
    let heap = IndexedMinHeap::<u32, 2>::new();
    heap.insert(0, 10);
    heap.insert(1, 20);
    heap.remove(0);
    heap.insert(0, 30);
    assert_yields!(drain(&heap), [(1, 20), (0, 30)]);
}

#[test]
fn interleaved_inserts_and_removals_drain_in_key_order() {
    let heap = IndexedMinHeap::<u32, 16>::new();
    let keys: [u32; 16] = core::array::from_fn(|i| u32::try_from(i * 7 % 16).unwrap() * 3);
    for (idx, key) in (0..).zip(keys) {
        heap.insert(idx, key);
    }
    let removed = [3, 0, 15, 8, 9];
    for idx in removed {
        heap.remove(idx);
    }
    let remaining = (0..).zip(keys).filter(|(idx, _)| !removed.contains(idx));
    let mut expected = [(0, 0); 11];
    for (expected, entry) in expected.iter_mut().zip(remaining) {
        *expected = entry;
    }
    expected.sort_unstable_by_key(|&(_, key)| key);
    assert_yields!(drain(&heap), expected);
}
