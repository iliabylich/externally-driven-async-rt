use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering::Relaxed};

use crate::tasks::wake_queue::{NONE, UNTRACKED};

pub(crate) struct Link {
    queue: AtomicPtr<()>,
    slot: u32,
    queued: AtomicBool,
    next: AtomicU32,
    generation: AtomicU64,
}

impl Link {
    pub(crate) const fn new(slot: u32) -> Self {
        Self {
            queue: AtomicPtr::new(core::ptr::null_mut()),
            slot,
            queued: AtomicBool::new(false),
            next: AtomicU32::new(NONE),
            generation: AtomicU64::new(UNTRACKED),
        }
    }

    pub(crate) fn queue(&self) -> *mut () {
        self.queue.load(Relaxed)
    }

    pub(super) fn set_queue(&self, queue: *mut ()) {
        self.queue.store(queue, Relaxed);
    }

    pub(crate) fn slot(&self) -> u32 {
        self.slot
    }

    pub(crate) fn is_queued(&self) -> bool {
        self.queued.load(Relaxed)
    }

    pub(crate) fn set_queued(&self, queued: bool) {
        self.queued.store(queued, Relaxed);
    }

    pub(crate) fn next(&self) -> u32 {
        self.next.load(Relaxed)
    }

    pub(crate) fn set_next(&self, next: u32) {
        self.next.store(next, Relaxed);
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Relaxed)
    }

    pub(crate) fn set_generation(&self, generation: u64) {
        self.generation.store(generation, Relaxed);
    }
}
