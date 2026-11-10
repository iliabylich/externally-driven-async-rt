use core::cell::Cell;

use rustix::fd::RawFd;

use crate::{
    pool::Index,
    selector::{Direction, Directions, IoChange, Token},
    waker_slot::WakerSlot,
};

pub(crate) struct IoWaiter {
    pub(super) fd: Cell<RawFd>,
    pub(super) registered: Cell<Directions>,
    pub(super) armed: Cell<Directions>,
    pub(super) reader: WakerSlot,
    pub(super) writer: WakerSlot,
}

impl IoWaiter {
    pub(super) fn waker(&self, direction: Direction) -> &WakerSlot {
        match direction {
            Direction::Read => &self.reader,
            Direction::Write => &self.writer,
        }
    }

    pub(super) fn interest(&self) -> Directions {
        [Direction::Read, Direction::Write]
            .into_iter()
            .filter(|direction| self.waker(*direction).is_set())
            .fold(Directions::Empty, Directions::with)
    }

    pub(super) fn change(&self, index: IoIndex) -> IoChange {
        IoChange {
            token: Token(index.to_bits()),
            fd: self.fd.get(),
            interest: self.interest(),
        }
    }
}

pub(crate) type IoIndex = Index<IoWaiter>;
