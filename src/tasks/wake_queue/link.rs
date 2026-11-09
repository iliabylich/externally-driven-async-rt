use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64};

pub(super) struct Link {
    pub(super) queue: AtomicPtr<()>,
    pub(super) slot: u32,
    pub(super) queued: AtomicBool,
    pub(super) next: AtomicU32,
    pub(super) generation: AtomicU64,
}
