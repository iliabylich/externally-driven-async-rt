use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering::Relaxed};

use crate::tasks::{
    WakeQueue,
    wake_queue::{NONE, UNTRACKED},
};

pub(crate) struct Link<const N: usize> {
    queue: AtomicPtr<WakeQueue<N>>,
    slot: u32,
    queued: AtomicBool,
    next: AtomicU32,
    generation: AtomicU64,
}

impl<const N: usize> Link<N> {
    pub(crate) const fn new(slot: u32) -> Self {
        Self {
            queue: AtomicPtr::new(core::ptr::null_mut()),
            slot,
            queued: AtomicBool::new(false),
            next: AtomicU32::new(NONE),
            generation: AtomicU64::new(UNTRACKED),
        }
    }

    pub(crate) fn queue(&self) -> Option<&'static WakeQueue<N>> {
        // SAFETY: `queue` is null or was stored by `set_queue` from a `&'static WakeQueue<N>`.
        unsafe { self.queue.load(Relaxed).cast_const().as_ref() }
    }

    pub(crate) fn set_queue(&self, queue: &'static WakeQueue<N>) {
        self.queue
            .store(core::ptr::from_ref(queue).cast_mut(), Relaxed);
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
