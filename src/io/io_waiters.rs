use core::{cell::Cell, task::Waker};

use rustix::fd::RawFd;

use crate::{
    io::{IoIndex, IoWaiter},
    pool::ObjectPool,
    selector::{Direction, Directions, IoChange, Token},
    waker_slot::WakerSlot,
};

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
                        registered: Cell::new(Directions::Empty),
                        armed: Cell::new(Directions::Empty),
                        reader: WakerSlot::new(),
                        writer: WakerSlot::new(),
                    }
                }; N],
            ),
        }
    }

    pub(crate) fn register(
        &self,
        cached: &Cell<Option<IoIndex>>,
        fd: RawFd,
        direction: Direction,
    ) -> IoIndex {
        let index = cached
            .get()
            .filter(|index| self.pool.get(*index).is_some_and(|io| io.fd.get() == fd))
            .unwrap_or_else(|| self.alloc(fd));
        cached.set(Some(index));
        let io = self.pool.get(index).expect("IO is not registered");
        let registered = io.registered.get();
        assert!(
            !registered.contains(direction),
            "fd {fd} already has a {direction:?} registration"
        );
        io.registered.set(registered.with(direction));
        index
    }

    fn alloc(&self, fd: RawFd) -> IoIndex {
        let Some((index, io)) = self.pool.alloc() else {
            panic!("more than {N} concurrently registered fds");
        };
        io.fd.set(fd);
        io.registered.set(Directions::Empty);
        io.armed.set(Directions::Empty);
        io.reader.take();
        io.writer.take();
        index
    }

    pub(crate) fn release(&self, index: IoIndex, direction: Direction) -> Option<IoChange> {
        let io = self.pool.get(index)?;
        io.waker(direction).take();
        io.registered.set(io.registered.get().without(direction));
        let change = io.armed.get().contains(direction).then(|| {
            let change = io.change(index);
            io.armed.set(change.interest);
            change
        });
        if io.registered.get() == Directions::Empty {
            self.pool.free(index);
        }
        change
    }

    pub(crate) fn set_waker(&self, index: IoIndex, direction: Direction, waker: &Waker) {
        let io = self.pool.get(index).expect("IO is not registered");
        io.waker(direction).store(waker);
    }

    pub(crate) fn remove_waker(&self, index: IoIndex, direction: Direction) {
        if let Some(io) = self.pool.get(index) {
            io.waker(direction).take();
        }
    }

    pub(crate) fn has_waker(&self, index: IoIndex, direction: Direction) -> bool {
        self.pool
            .get(index)
            .is_some_and(|io| io.waker(direction).is_set())
    }

    pub(crate) fn wake_ready(&self, token: Token, ready: Directions) {
        let Some(io) = self.pool.get(IoIndex::from_bits(token.0)) else {
            return;
        };
        io.armed.set(Directions::Empty);
        for direction in [Direction::Read, Direction::Write] {
            if ready.contains(direction)
                && let Some(waker) = io.waker(direction).take()
            {
                waker.wake();
            }
        }
    }

    pub(crate) fn pending_changes(&self) -> impl Iterator<Item = IoChange> + '_ {
        self.pool
            .iter()
            .filter(|(_, io)| io.interest() != io.armed.get())
            .map(|(index, io)| io.change(index))
    }

    pub(crate) fn mark_flushed(&self) {
        for (_, io) in self.pool.iter() {
            io.armed.set(io.interest());
        }
    }

    pub(crate) fn clear(&self) {
        for (index, io) in self.pool.iter() {
            io.reader.take();
            io.writer.take();
            self.pool.free(index);
        }
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.pool.iter().count()
    }
}

#[cfg(test)]
mod tests {
    use core::{cell::Cell, task::Waker};

    use crate::{
        io::{IoIndex, IoWaiters},
        selector::{Direction, Directions, IoChange, Token},
        testing::assert_yields,
    };

    #[track_caller]
    fn assert_pending_changes<const N: usize>(
        io_waiters: &IoWaiters<N>,
        expected: impl IntoIterator<Item = IoChange>,
    ) {
        assert_yields!(io_waiters.pending_changes(), expected);
    }

    fn change(index: IoIndex, fd: i32, interest: Directions) -> IoChange {
        IoChange {
            token: Token(index.to_bits()),
            fd,
            interest,
        }
    }

    #[test]
    fn register_wait_release() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let index = io_waiters.register(cached, 3, Direction::Write);
        assert!(!io_waiters.has_waker(index, Direction::Write));
        io_waiters.set_waker(index, Direction::Write, Waker::noop());
        assert!(io_waiters.has_waker(index, Direction::Write));
        assert_eq!(io_waiters.release(index, Direction::Write), None);
        assert!(!io_waiters.has_waker(index, Direction::Write));
        assert_eq!(io_waiters.count(), 0);
        let reused = io_waiters.register(cached, 3, Direction::Read);
        assert_ne!(reused, index);
    }

    #[test]
    fn both_directions_of_one_fd_share_an_entry() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let reader = io_waiters.register(cached, 3, Direction::Read);
        let writer = io_waiters.register(cached, 3, Direction::Write);
        assert_eq!(reader, writer);
        assert_eq!(io_waiters.count(), 1);
    }

    #[test]
    fn a_cached_index_of_another_fd_is_not_reused() {
        let io_waiters = &IoWaiters::<2>::new();
        let cached = &Cell::new(None);
        let first = io_waiters.register(cached, 3, Direction::Read);
        let second = io_waiters.register(cached, 4, Direction::Read);
        assert_ne!(first, second);
        assert_eq!(cached.get(), Some(second));
    }

    #[test]
    #[should_panic(expected = "fd 3 already has a Read registration")]
    fn a_direction_is_registered_once_per_fd() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        io_waiters.register(cached, 3, Direction::Read);
        io_waiters.register(cached, 3, Direction::Read);
    }

    #[test]
    fn changes_follow_the_stored_wakers() {
        let io_waiters = &IoWaiters::<1>::new();
        let index = io_waiters.register(&Cell::new(None), 3, Direction::Read);
        assert_pending_changes(io_waiters, []);

        io_waiters.set_waker(index, Direction::Read, Waker::noop());
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Read)]);
        io_waiters.mark_flushed();
        assert_pending_changes(io_waiters, []);

        io_waiters.remove_waker(index, Direction::Read);
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Empty)]);
    }

    #[test]
    fn one_change_per_fd_with_the_combined_interest() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let index = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.register(cached, 3, Direction::Write);
        io_waiters.set_waker(index, Direction::Read, Waker::noop());
        io_waiters.set_waker(index, Direction::Write, Waker::noop());
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Both)]);
    }

    #[test]
    fn releasing_an_armed_direction_returns_the_remaining_interest() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let index = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.register(cached, 3, Direction::Write);
        io_waiters.set_waker(index, Direction::Read, Waker::noop());
        io_waiters.set_waker(index, Direction::Write, Waker::noop());
        io_waiters.mark_flushed();

        assert_eq!(
            io_waiters.release(index, Direction::Read),
            Some(change(index, 3, Directions::Write))
        );
        assert_eq!(io_waiters.release(index, Direction::Read), None);
        assert_eq!(io_waiters.count(), 1);
        assert_eq!(
            io_waiters.release(index, Direction::Write),
            Some(change(index, 3, Directions::Empty))
        );
        assert_eq!(io_waiters.count(), 0);
    }

    #[test]
    fn an_event_wakes_only_the_ready_direction_and_rearms_the_other() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let index = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.register(cached, 3, Direction::Write);
        io_waiters.set_waker(index, Direction::Read, Waker::noop());
        io_waiters.set_waker(index, Direction::Write, Waker::noop());
        io_waiters.mark_flushed();

        io_waiters.wake_ready(Token(index.to_bits()), Directions::Read);
        assert!(!io_waiters.has_waker(index, Direction::Read));
        assert!(io_waiters.has_waker(index, Direction::Write));
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Write)]);
    }

    #[test]
    fn an_event_for_a_released_entry_is_ignored() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let stale = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.release(stale, Direction::Read);
        let index = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.set_waker(index, Direction::Read, Waker::noop());
        io_waiters.mark_flushed();

        io_waiters.wake_ready(Token(stale.to_bits()), Directions::Both);
        assert!(io_waiters.has_waker(index, Direction::Read));
        assert_pending_changes(io_waiters, []);
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrently registered fds")]
    fn capacity_is_bounded() {
        let io_waiters = &IoWaiters::<1>::new();
        io_waiters.register(&Cell::new(None), 3, Direction::Read);
        io_waiters.register(&Cell::new(None), 4, Direction::Read);
    }
}
