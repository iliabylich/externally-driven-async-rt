use core::cell::Cell;

use rustix::fd::RawFd;

use crate::{
    selector::{Direction, Directions, IoChange},
    waker_slot::WakerSlot,
};

#[must_use]
pub(crate) struct IoWaiter {
    registered: Cell<Directions>,
    armed: Cell<Directions>,
    reader: WakerSlot,
    writer: WakerSlot,
}

impl IoWaiter {
    pub(crate) const fn new() -> Self {
        Self {
            registered: Cell::new(Directions::Empty),
            armed: Cell::new(Directions::Empty),
            reader: WakerSlot::new(),
            writer: WakerSlot::new(),
        }
    }

    pub(crate) fn register(&self, direction: Direction) {
        self.waker(direction).take();
        self.registered.set(self.registered.get().with(direction));
    }

    pub(crate) fn release(&self, fd: RawFd, direction: Direction) -> Option<IoChange> {
        self.waker(direction).take();
        self.registered
            .set(self.registered.get().without(direction));
        self.armed.get().contains(direction).then(|| {
            self.mark_flushed();
            self.change(fd)
        })
    }

    pub(crate) fn is_registered(&self) -> bool {
        self.registered.get() != Directions::Empty
    }

    pub(crate) const fn waker(&self, direction: Direction) -> &WakerSlot {
        match direction {
            Direction::Read => &self.reader,
            Direction::Write => &self.writer,
        }
    }

    pub(crate) fn wake(&self, ready: Directions) {
        self.armed.set(Directions::Empty);
        for direction in [Direction::Read, Direction::Write] {
            if ready.contains(direction)
                && let Some(waker) = self.waker(direction).take()
            {
                waker.wake();
            }
        }
    }

    pub(crate) fn pending_change(&self, fd: RawFd) -> Option<IoChange> {
        (self.interest() != self.armed.get()).then(|| self.change(fd))
    }

    pub(crate) fn mark_flushed(&self) {
        self.armed.set(self.interest());
    }

    pub(crate) fn drop_wakers(&self) {
        self.reader.take();
        self.writer.take();
    }

    fn interest(&self) -> Directions {
        [Direction::Read, Direction::Write]
            .into_iter()
            .filter(|direction| self.waker(*direction).is_set())
            .fold(Directions::Empty, Directions::with)
    }

    fn change(&self, fd: RawFd) -> IoChange {
        IoChange {
            fd,
            interest: self.interest(),
        }
    }
}
