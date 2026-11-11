use core::cell::Cell;

use rustix::fd::RawFd;

use crate::{
    io::{IoIndex, IoWaiter},
    pool::ObjectPool,
    selector::{Direction, Directions, IoChange},
};

pub(crate) struct IoWaiters<const N: usize> {
    pool: ObjectPool<IoWaiter, N>,
}

impl<const N: usize> IoWaiters<N> {
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new([const { IoWaiter::new() }; N]),
        }
    }

    pub(crate) fn register(
        &self,
        cached: &Cell<Option<IoIndex>>,
        fd: RawFd,
        direction: Direction,
    ) -> (IoIndex, &IoWaiter) {
        let (index, io) = match cached
            .get()
            .and_then(|index| Some((index, self.pool.get(index)?)))
            .filter(|(_, io)| io.fd() == fd)
        {
            Some(entry) => entry,
            None => self.alloc(fd),
        };
        cached.set(Some(index));
        io.register(direction);
        (index, io)
    }

    fn alloc(&self, fd: RawFd) -> (IoIndex, &IoWaiter) {
        let Some((index, io)) = self.pool.alloc() else {
            panic!("more than {N} concurrently registered fds");
        };
        io.reset(fd);
        (index, io)
    }

    pub(crate) fn release(&self, index: IoIndex, direction: Direction) -> Option<IoChange> {
        let io = self.pool.get(index)?;
        let change = io.release(index, direction);
        if !io.is_registered() {
            self.pool.free(index);
        }
        change
    }

    pub(crate) fn wake_ready(&self, index: IoIndex, ready: Directions) {
        if let Some(io) = self.pool.get(index) {
            io.wake(ready);
        }
    }

    pub(crate) fn pending_changes(&self) -> impl Iterator<Item = IoChange> + '_ {
        self.pool
            .iter()
            .filter_map(|(index, io)| io.pending_change(index))
    }

    pub(crate) fn mark_flushed(&self) {
        for (_, io) in self.pool.iter() {
            io.mark_flushed();
        }
    }

    pub(crate) fn clear(&self) {
        for (index, io) in self.pool.iter() {
            io.clear();
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
        selector::{Direction, Directions, IoChange},
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
        IoChange::new(index.to_bits(), fd, interest)
    }

    #[test]
    fn register_wait_release() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (index, io) = io_waiters.register(cached, 3, Direction::Write);
        assert!(!io.waker(Direction::Write).is_set());
        io.waker(Direction::Write).store(Waker::noop());
        assert!(io.waker(Direction::Write).is_set());
        assert_eq!(io_waiters.release(index, Direction::Write), None);
        assert!(!io.waker(Direction::Write).is_set());
        assert_eq!(io_waiters.count(), 0);
        let (reused, _) = io_waiters.register(cached, 3, Direction::Read);
        assert_ne!(reused, index);
    }

    #[test]
    fn both_directions_of_one_fd_share_an_entry() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (reader, _) = io_waiters.register(cached, 3, Direction::Read);
        let (writer, _) = io_waiters.register(cached, 3, Direction::Write);
        assert_eq!(reader, writer);
        assert_eq!(io_waiters.count(), 1);
    }

    #[test]
    fn a_cached_index_of_another_fd_is_not_reused() {
        let io_waiters = &IoWaiters::<2>::new();
        let cached = &Cell::new(None);
        let (first, _) = io_waiters.register(cached, 3, Direction::Read);
        let (second, _) = io_waiters.register(cached, 4, Direction::Read);
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
        let (index, io) = io_waiters.register(&Cell::new(None), 3, Direction::Read);
        assert_pending_changes(io_waiters, []);

        io.waker(Direction::Read).store(Waker::noop());
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Read)]);
        io_waiters.mark_flushed();
        assert_pending_changes(io_waiters, []);

        io.waker(Direction::Read).take();
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Empty)]);
    }

    #[test]
    fn one_change_per_fd_with_the_combined_interest() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (index, io) = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.register(cached, 3, Direction::Write);
        io.waker(Direction::Read).store(Waker::noop());
        io.waker(Direction::Write).store(Waker::noop());
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Both)]);
    }

    #[test]
    fn releasing_an_armed_direction_returns_the_remaining_interest() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (index, io) = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.register(cached, 3, Direction::Write);
        io.waker(Direction::Read).store(Waker::noop());
        io.waker(Direction::Write).store(Waker::noop());
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
        let (index, io) = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.register(cached, 3, Direction::Write);
        io.waker(Direction::Read).store(Waker::noop());
        io.waker(Direction::Write).store(Waker::noop());
        io_waiters.mark_flushed();

        io_waiters.wake_ready(index, Directions::Read);
        assert!(!io.waker(Direction::Read).is_set());
        assert!(io.waker(Direction::Write).is_set());
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Write)]);
    }

    #[test]
    fn an_event_for_a_released_entry_is_ignored() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (stale, _) = io_waiters.register(cached, 3, Direction::Read);
        io_waiters.release(stale, Direction::Read);
        let (_, io) = io_waiters.register(cached, 3, Direction::Read);
        io.waker(Direction::Read).store(Waker::noop());
        io_waiters.mark_flushed();

        io_waiters.wake_ready(stale, Directions::Both);
        assert!(io.waker(Direction::Read).is_set());
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
