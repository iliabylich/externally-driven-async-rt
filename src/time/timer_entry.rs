use core::cell::Cell;

use crate::{pool::Index, time::Instant, waker_slot::WakerSlot};

pub(crate) struct TimerEntry {
    pub(super) deadline: Cell<Instant>,
    pub(super) seq: Cell<u64>,
    pub(super) waker: WakerSlot,
}

pub(crate) type TimerIndex = Index<TimerEntry>;
