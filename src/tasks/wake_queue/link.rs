use core::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};

use crate::tasks::wake_queue::NONE;

pub(crate) struct Link {
    slot: u32,
    queued: AtomicBool,
    next: AtomicU32,
}

impl Link {
    pub(crate) const fn new(slot: u32) -> Self {
        Self {
            slot,
            queued: AtomicBool::new(false),
            next: AtomicU32::new(NONE),
        }
    }

    pub(crate) const fn slot(&self) -> u32 {
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
}
