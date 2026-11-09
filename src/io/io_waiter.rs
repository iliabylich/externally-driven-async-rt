use core::cell::Cell;

use rustix::fd::RawFd;

use crate::{
    pool::Index,
    selector::{Direction, IoAction, IoChange, Token},
    waker_slot::WakerSlot,
};

pub(crate) struct IoWaiter {
    pub(super) fd: Cell<RawFd>,
    pub(super) direction: Cell<Direction>,
    pub(super) waker: WakerSlot,
    pub(super) armed: Cell<bool>,
}

impl IoWaiter {
    pub(super) fn change(&self, index: IoIndex, action: IoAction) -> IoChange {
        IoChange {
            token: Token(index.to_bits()),
            fd: self.fd.get(),
            direction: self.direction.get(),
            action,
        }
    }

    pub(super) fn pending_change(&self, index: IoIndex) -> Option<IoChange> {
        let action = match (self.waker.is_set(), self.armed.get()) {
            (true, false) => IoAction::Arm,
            (false, true) => IoAction::Disarm,
            (true, true) | (false, false) => return None,
        };
        Some(self.change(index, action))
    }
}

pub(crate) type IoIndex = Index<IoWaiter>;
