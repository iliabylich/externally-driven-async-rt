use core::{future::Future, ptr::NonNull};

#[derive(Clone, Copy)]
#[must_use]
pub(crate) enum TaskSlot {
    Empty,
    Idle(NonNull<dyn Future<Output = ()>>),
    Polling,
    Dropping,
}
