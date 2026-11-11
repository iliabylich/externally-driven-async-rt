use core::cell::Cell;

use rustix::fd::RawFd;

use crate::{
    pool::Index,
    selector::{Direction, Directions, IoChange},
    waker_slot::WakerSlot,
};

pub(crate) struct IoWaiter {
    fd: Cell<RawFd>,
    registered: Cell<Directions>,
    armed: Cell<Directions>,
    reader: WakerSlot,
    writer: WakerSlot,
}

impl IoWaiter {
    pub(crate) const fn new() -> Self {
        Self {
            fd: Cell::new(-1),
            registered: Cell::new(Directions::Empty),
            armed: Cell::new(Directions::Empty),
            reader: WakerSlot::new(),
            writer: WakerSlot::new(),
        }
    }

    pub(crate) fn reset(&self, fd: RawFd) {
        self.fd.set(fd);
        self.registered.set(Directions::Empty);
        self.armed.set(Directions::Empty);
        self.clear();
    }

    pub(crate) fn fd(&self) -> RawFd {
        self.fd.get()
    }

    pub(crate) fn register(&self, direction: Direction) {
        let registered = self.registered.get();
        assert!(
            !registered.contains(direction),
            "fd {} already has a {direction:?} registration",
            self.fd()
        );
        self.registered.set(registered.with(direction));
    }

    pub(crate) fn release(&self, index: IoIndex, direction: Direction) -> Option<IoChange> {
        self.waker(direction).take();
        self.registered
            .set(self.registered.get().without(direction));
        self.armed.get().contains(direction).then(|| {
            self.mark_flushed();
            self.change(index)
        })
    }

    pub(crate) fn is_registered(&self) -> bool {
        self.registered.get() != Directions::Empty
    }

    pub(crate) fn waker(&self, direction: Direction) -> &WakerSlot {
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

    pub(crate) fn pending_change(&self, index: IoIndex) -> Option<IoChange> {
        (self.interest() != self.armed.get()).then(|| self.change(index))
    }

    pub(crate) fn mark_flushed(&self) {
        self.armed.set(self.interest());
    }

    pub(crate) fn clear(&self) {
        self.reader.take();
        self.writer.take();
    }

    fn interest(&self) -> Directions {
        [Direction::Read, Direction::Write]
            .into_iter()
            .filter(|direction| self.waker(*direction).is_set())
            .fold(Directions::Empty, Directions::with)
    }

    fn change(&self, index: IoIndex) -> IoChange {
        IoChange::new(index.to_bits(), self.fd(), self.interest())
    }
}

pub(crate) type IoIndex = Index<IoWaiter>;
