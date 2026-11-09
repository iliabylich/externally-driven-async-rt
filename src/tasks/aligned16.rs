use core::{cell::UnsafeCell, mem::MaybeUninit};

#[repr(C, align(16))]
pub(super) struct Aligned16<const SIZE: usize>(pub(super) UnsafeCell<MaybeUninit<[u8; SIZE]>>);
