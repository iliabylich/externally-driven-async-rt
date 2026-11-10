use core::{cell::UnsafeCell, mem::MaybeUninit, ptr::NonNull};

#[repr(C, align(16))]
pub(crate) struct Aligned16<const SIZE: usize>(UnsafeCell<MaybeUninit<[u8; SIZE]>>);

impl<const SIZE: usize> Aligned16<SIZE> {
    pub(crate) const fn new() -> Self {
        Self(UnsafeCell::new(MaybeUninit::uninit()))
    }

    pub(crate) fn as_ptr(&self) -> NonNull<u8> {
        NonNull::from(&self.0).cast()
    }
}
