use super::{Popped, Queue};
use crate::testing::assert_yields;

#[test]
fn pops_in_push_order() {
    let queue = Queue::<4>::new();
    for idx in [2, 0, 3] {
        queue.push(idx);
    }
    assert!(!queue.is_empty());
    assert_yields!(
        core::iter::from_fn(|| queue.pop()),
        [Popped::Slot(2), Popped::Slot(0), Popped::Slot(3)]
    );
    assert!(queue.is_empty());
}

#[test]
fn a_queued_slot_is_pushed_once() {
    let queue = Queue::<2>::new();
    queue.push(0);
    queue.push(1);
    queue.push(0);
    assert_yields!(
        core::iter::from_fn(|| queue.pop()),
        [Popped::Slot(0), Popped::Slot(1)]
    );
    queue.push(0);
    assert_yields!(core::iter::from_fn(|| queue.pop()), [Popped::Slot(0)]);
}

#[test]
fn re_push_during_draining_goes_to_the_back() {
    let queue = Queue::<2>::new();
    queue.push(0);
    queue.push(1);
    assert_eq!(queue.pop(), Some(Popped::Slot(0)));
    queue.push(0);
    assert_yields!(
        core::iter::from_fn(|| queue.pop()),
        [Popped::Slot(1), Popped::Slot(0)]
    );
}

#[test]
fn the_extra_slot_is_ordered_with_the_indexed_ones() {
    let queue = Queue::<2>::new();
    queue.push(1);
    queue.push_extra();
    queue.push(0);
    queue.push_extra();
    assert_yields!(
        core::iter::from_fn(|| queue.pop()),
        [Popped::Slot(1), Popped::Extra, Popped::Slot(0)]
    );
    assert!(queue.is_empty());
}
