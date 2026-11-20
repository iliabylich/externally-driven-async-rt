use core::{
    cell::{Cell, UnsafeCell},
    mem::MaybeUninit,
    ptr::NonNull,
};

#[repr(C, align(16))]
#[must_use]
pub(crate) struct TaskMemory<const SIZE: usize> {
    bytes: UnsafeCell<MaybeUninit<[u8; SIZE]>>,
    occupied: Cell<bool>,
}

impl<const SIZE: usize> TaskMemory<SIZE> {
    pub(crate) const fn new() -> Self {
        Self {
            bytes: UnsafeCell::new(MaybeUninit::uninit()),
            occupied: Cell::new(false),
        }
    }

    pub(crate) const fn occupied(&self) -> &Cell<bool> {
        &self.occupied
    }

    pub(crate) fn as_ptr(&self) -> NonNull<u8> {
        NonNull::from(&self.bytes).cast()
    }
}
