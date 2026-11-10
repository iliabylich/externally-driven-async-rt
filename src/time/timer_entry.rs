use core::{cell::Cell, task::Waker};

use crate::{pool::Index, time::Instant, waker_slot::WakerSlot};

pub(crate) struct TimerEntry {
    deadline: Cell<Instant>,
    seq: Cell<u64>,
    waker: WakerSlot,
}

impl TimerEntry {
    pub(crate) const fn new() -> Self {
        Self {
            deadline: Cell::new(Instant::ZERO),
            seq: Cell::new(0),
            waker: WakerSlot::new(),
        }
    }

    pub(crate) fn arm(&self, deadline: Instant, seq: u64, waker: Waker) {
        self.deadline.set(deadline);
        self.seq.set(seq);
        self.waker.set(waker);
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline.get()
    }

    pub(crate) fn expiry(&self) -> (Instant, u64) {
        (self.deadline.get(), self.seq.get())
    }

    pub(crate) fn set_waker(&self, waker: &Waker) {
        self.waker.store(waker);
    }

    pub(crate) fn take_waker(&self) -> Option<Waker> {
        self.waker.take()
    }
}

pub(crate) type TimerIndex = Index<TimerEntry>;
