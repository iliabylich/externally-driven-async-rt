use core::{alloc::Layout, cell::Cell, ptr::NonNull, task::Waker};

use crate::{
    instant::Instant,
    pool::{Index, ObjectPool},
    storage::store_waker,
};

// Pool values are `Cell`s (see `ObjectPool`); a vacant slot holds a stale or default value.
#[derive(Default)]
pub(crate) struct Timer {
    deadline: Cell<Instant>,
    // Insertion order, so timers with equal deadlines expire in the order they were created.
    seq: Cell<u64>,
    waker: Cell<Option<Waker>>,
}

pub(crate) type TimerIndex = Index<Timer>;

pub(crate) struct Timers {
    pool: &'static ObjectPool<Timer>,
    next_seq: Cell<u64>,
}

impl Timers {
    pub(crate) fn layout(len: usize) -> Layout {
        ObjectPool::<Timer>::layout(len)
    }

    /// # Safety
    ///
    /// As `ObjectPool::init`, with the memory valid (and otherwise unused) forever.
    pub(crate) unsafe fn init(ptr: NonNull<u8>, len: usize) -> Self {
        Self {
            // SAFETY: guaranteed by the caller.
            pool: unsafe { ObjectPool::init(ptr, len) },
            next_seq: Cell::new(0),
        }
    }

    pub(crate) fn alloc(&self, deadline: Instant, waker: Waker) -> TimerIndex {
        let Some((index, timer)) = self.pool.alloc() else {
            panic!("more than {} concurrent sleeps", self.pool.capacity());
        };
        let seq = self.next_seq.get();
        self.next_seq.set(seq + 1);
        timer.deadline.set(deadline);
        timer.seq.set(seq);
        timer.waker.set(Some(waker));
        index
    }

    pub(crate) fn set_waker(&self, index: TimerIndex, waker: &Waker) {
        let timer = self
            .pool
            .get(index)
            .expect("a pending sleep's timer is in storage");
        store_waker(&timer.waker, waker);
    }

    pub(crate) fn release(&self, index: TimerIndex) {
        if let Some(timer) = self.pool.get(index) {
            timer.waker.take();
            self.pool.free(index);
        }
    }

    /// Releases every timer, so the pool holds nothing that needs dropping.
    pub(crate) fn clear(&self) {
        for (index, _) in self.pool.iter() {
            self.release(index);
        }
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.pool
            .iter()
            .map(|(_, timer)| timer.deadline.get())
            .min()
    }

    pub(crate) fn take_expired(&self, now: Instant) -> Option<Waker> {
        let (index, _) = self
            .pool
            .iter()
            .filter(|(_, timer)| timer.deadline.get() <= now)
            .min_by_key(|(_, timer)| (timer.deadline.get(), timer.seq.get()))?;
        let waker = self.pool.get(index)?.waker.take();
        self.pool.free(index);
        waker
    }
}
