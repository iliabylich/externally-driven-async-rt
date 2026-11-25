use core::{future::Future, ptr::NonNull};

#[derive(Clone, Copy)]
#[must_use]
pub(super) enum TaskSlot {
    Empty,
    Idle(NonNull<dyn Future<Output = ()>>),
    Polling,
    Panicked(NonNull<dyn Future<Output = ()>>),
    Dropping,
}
