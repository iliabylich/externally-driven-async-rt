use core::cell::Cell;

use rustix::fd::RawFd;

use crate::{
    selector::{Direction, Directions, IoChange},
    waker_slot::WakerSlot,
};

#[must_use]
pub(super) struct IoWaiter {
    registered: Cell<Directions>,
    armed: Cell<Directions>,
    reader: WakerSlot,
    writer: WakerSlot,
}

impl IoWaiter {
    pub(super) const fn new() -> Self {
        Self {
            registered: Cell::new(Directions::Empty),
            armed: Cell::new(Directions::Empty),
            reader: WakerSlot::new(),
            writer: WakerSlot::new(),
        }
    }

    pub(super) fn register(&self, direction: Direction) {
        self.waker(direction).take();
        self.registered.set(self.registered.get().with(direction));
    }

    pub(super) fn release(&self, fd: RawFd, direction: Direction) -> Option<IoChange> {
        self.waker(direction).take();
        self.registered
            .set(self.registered.get().without(direction));
        self.armed.get().contains(direction).then(|| {
            self.mark_flushed();
            self.change(fd)
        })
    }

    pub(super) const fn is_registered(&self) -> bool {
        !matches!(self.registered.get(), Directions::Empty)
    }

    pub(super) const fn waker(&self, direction: Direction) -> &WakerSlot {
        match direction {
            Direction::Read => &self.reader,
            Direction::Write => &self.writer,
        }
    }

    pub(super) fn wake(&self, ready: Directions) {
        self.armed.set(Directions::Empty);
        for direction in [Direction::Read, Direction::Write] {
            if ready.contains(direction)
                && let Some(waker) = self.waker(direction).take()
            {
                waker.wake();
            }
        }
    }

    pub(super) fn pending_change(&self, fd: RawFd) -> Option<IoChange> {
        (self.interest() != self.armed.get()).then(|| self.change(fd))
    }

    pub(super) const fn mark_flushed(&self) {
        let _ = self.armed.replace(self.interest());
    }

    pub(super) fn drop_wakers(&self) {
        self.reader.take();
        self.writer.take();
    }

    const fn interest(&self) -> Directions {
        let mut interest = Directions::Empty;
        if self.reader.is_set() {
            interest = interest.with(Direction::Read);
        }
        if self.writer.is_set() {
            interest = interest.with(Direction::Write);
        }
        interest
    }

    const fn change(&self, fd: RawFd) -> IoChange {
        IoChange {
            fd,
            interest: self.interest(),
        }
    }
}
