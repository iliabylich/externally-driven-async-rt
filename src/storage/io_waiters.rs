use core::{cell::Cell, task::Waker};

use rustix::fd::RawFd;

use crate::{
    pool::{Index, ObjectPool},
    selectors::{Direction, IoAction, IoChange, Token},
};

pub(crate) struct IoWaiter {
    fd: Cell<RawFd>,
    direction: Cell<Direction>,
    waker: Cell<Option<Waker>>,
    armed: Cell<bool>,
}

impl IoWaiter {
    fn is_waiting(&self) -> bool {
        let value = self.waker.take();
        let some = value.is_some();
        self.waker.set(value);
        some
    }

    fn change(&self, index: IoIndex, action: IoAction) -> IoChange {
        IoChange {
            token: Token(index.to_bits()),
            fd: self.fd.get(),
            direction: self.direction.get(),
            action,
        }
    }

    fn pending_change(&self, index: IoIndex) -> Option<IoChange> {
        let action = match (self.is_waiting(), self.armed.get()) {
            (true, false) => IoAction::Arm,
            (false, true) => IoAction::Disarm,
            (true, true) | (false, false) => return None,
        };
        Some(self.change(index, action))
    }
}

pub(crate) type IoIndex = Index<IoWaiter>;

pub(crate) struct IoWaiters<const N: usize> {
    pool: ObjectPool<IoWaiter, N>,
}

impl<const N: usize> IoWaiters<N> {
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new(
                [const {
                    IoWaiter {
                        fd: Cell::new(-1),
                        direction: Cell::new(Direction::Read),
                        waker: Cell::new(None),
                        armed: Cell::new(false),
                    }
                }; N],
            ),
        }
    }

    pub(crate) fn register(&self, fd: RawFd, direction: Direction) -> IoIndex {
        let Some((index, io)) = self.pool.alloc() else {
            panic!("more than {N} concurrent IO operations");
        };
        io.fd.set(fd);
        io.direction.set(direction);
        io.waker.set(None);
        io.armed.set(false);
        index
    }

    pub(crate) fn release(&self, index: IoIndex) -> Option<IoChange> {
        let io = self.pool.get(index)?;
        io.waker.take();
        let change = if io.armed.get() {
            Some(io.change(index, IoAction::Disarm))
        } else {
            None
        };
        self.pool.free(index);
        change
    }

    pub(crate) fn set_waker(&self, index: IoIndex, waker: &Waker) {
        let io = self.pool.get(index).expect("IO is not registered");
        let stored = match io.waker.take() {
            Some(mut stored) => {
                stored.clone_from(waker);
                stored
            }
            None => waker.clone(),
        };
        io.waker.set(Some(stored));
    }

    pub(crate) fn remove_waker(&self, index: IoIndex) {
        if let Some(io) = self.pool.get(index) {
            io.waker.take();
        }
    }

    pub(crate) fn has_waker(&self, index: IoIndex) -> bool {
        self.pool.get(index).is_some_and(IoWaiter::is_waiting)
    }

    pub(crate) fn take_waker(&self, token: Token, direction: Direction) -> Option<Waker> {
        let io = self.pool.get(IoIndex::from_bits(token.0))?;
        if io.direction.get() != direction {
            return None;
        }
        io.armed.set(false);
        io.waker.take()
    }

    pub(crate) fn pending_changes(&self) -> impl Iterator<Item = IoChange> + '_ {
        self.pool
            .iter()
            .filter_map(|(index, io)| io.pending_change(index))
    }

    pub(crate) fn mark_flushed(&self) {
        for (_, io) in self.pool.iter() {
            io.armed.set(io.is_waiting());
        }
    }

    pub(crate) fn clear(&self) {
        for (index, _) in self.pool.iter() {
            self.release(index);
        }
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.pool.iter().count()
    }
}

#[cfg(test)]
mod tests {
    use core::task::Waker;

    use crate::{
        selectors::{Direction, IoAction, IoChange, Token},
        storage::{IoIndex, IoWaiters},
        test_utils::{assert_yields, waker},
    };

    #[track_caller]
    fn assert_pending_changes<const N: usize>(
        io_waiters: &IoWaiters<N>,
        expected: impl IntoIterator<Item = IoChange>,
    ) {
        assert_yields(io_waiters.pending_changes(), expected);
    }

    #[test]
    fn register_wait_release() {
        let io_waiters = &IoWaiters::<1>::new();
        let index = io_waiters.register(3, Direction::Write);
        assert!(!io_waiters.has_waker(index));
        io_waiters.set_waker(index, Waker::noop());
        assert!(io_waiters.has_waker(index));
        assert_eq!(io_waiters.release(index), None);
        assert!(!io_waiters.has_waker(index));
        assert_eq!(io_waiters.count(), 0);
        let reused = io_waiters.register(3, Direction::Read);
        assert_ne!(reused, index);
    }

    #[test]
    fn changes_follow_the_stored_waker() {
        let io_waiters = &IoWaiters::<2>::new();
        let reader = io_waiters.register(3, Direction::Read);
        let writer = io_waiters.register(4, Direction::Write);
        assert_pending_changes(io_waiters, []);

        let change = |action| IoChange {
            token: Token(reader.to_bits()),
            fd: 3,
            direction: Direction::Read,
            action,
        };
        io_waiters.set_waker(reader, Waker::noop());
        assert_pending_changes(io_waiters, [change(IoAction::Arm)]);
        io_waiters.mark_flushed();
        assert_pending_changes(io_waiters, []);

        io_waiters.remove_waker(reader);
        assert_pending_changes(io_waiters, [change(IoAction::Disarm)]);
        assert_eq!(IoIndex::from_bits(reader.to_bits()), reader);
        assert!(!io_waiters.has_waker(writer));
        assert_eq!(io_waiters.count(), 2);
    }

    #[test]
    fn releasing_an_armed_registration_returns_its_disarm() {
        let io_waiters = &IoWaiters::<1>::new();
        let index = io_waiters.register(3, Direction::Read);
        io_waiters.set_waker(index, Waker::noop());
        io_waiters.mark_flushed();
        let disarm = IoChange {
            token: Token(index.to_bits()),
            fd: 3,
            direction: Direction::Read,
            action: IoAction::Disarm,
        };
        assert_eq!(io_waiters.release(index), Some(disarm));
        assert_eq!(io_waiters.release(index), None);
    }

    #[test]
    fn an_event_takes_the_waker_of_its_direction_only() {
        let io_waiters = &IoWaiters::<1>::new();
        let index = io_waiters.register(3, Direction::Read);
        io_waiters.set_waker(index, &waker(1));
        let token = Token(index.to_bits());
        assert!(io_waiters.take_waker(token, Direction::Write).is_none());
        let woken = io_waiters.take_waker(token, Direction::Read).unwrap();
        assert_eq!(woken.data().addr(), 1);
        assert!(!io_waiters.has_waker(index));
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent IO operations")]
    fn capacity_is_bounded() {
        let io_waiters = &IoWaiters::<1>::new();
        io_waiters.register(3, Direction::Read);
        io_waiters.register(3, Direction::Write);
    }
}
