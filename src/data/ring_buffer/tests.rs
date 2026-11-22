use super::RingBuffer;
use crate::testing::assert_yields;

#[test]
fn pops_values_in_push_order() {
    let buffer = RingBuffer::<u32, 3>::new();
    assert_eq!(buffer.push(1), Ok(()));
    assert_eq!(buffer.push(2), Ok(()));
    assert_eq!(buffer.push(3), Ok(()));
    assert_yields!(buffer.iter_removing(), [1, 2, 3]);
}

#[test]
fn pushing_into_a_full_buffer_returns_the_value() {
    let buffer = RingBuffer::<u32, 2>::new();
    assert_eq!(buffer.push(1), Ok(()));
    assert_eq!(buffer.push(2), Ok(()));
    assert_eq!(buffer.push(3), Err(3));
    assert_yields!(buffer.iter_removing(), [1, 2]);
}

#[test]
fn indices_wrap_around_the_capacity() {
    let buffer = RingBuffer::<u32, 2>::new();
    assert_eq!(buffer.push(1), Ok(()));
    assert_eq!(buffer.push(2), Ok(()));
    assert_eq!(buffer.pop(), Some(1));
    assert_eq!(buffer.push(3), Ok(()));
    assert_eq!(buffer.push(4), Err(4));
    assert_yields!(buffer.iter_removing(), [2, 3]);
}

#[test]
fn zero_sized_values_are_counted() {
    let buffer = RingBuffer::<(), 2>::new();
    assert_eq!(buffer.push(()), Ok(()));
    assert_eq!(buffer.push(()), Ok(()));
    assert_eq!(buffer.push(()), Err(()));
    assert_yields!(buffer.iter_removing(), [(), ()]);
}
