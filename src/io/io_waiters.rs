use core::cell::Cell;

use rustix::fd::RawFd;

use crate::{
    io::{FdsFullError, IoIndex, IoWaiter},
    pool::ObjectPool,
    selector::{Direction, Directions, IoChange},
};

#[must_use]
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
    ) -> Result<(IoIndex, &IoWaiter), FdsFullError> {
        if let Some(index) = cached.get()
            && let Some(io) = self.get(index)
            && io.fd() == fd
        {
            io.register(direction);
            return Ok((index, io));
        }
        let (index, io) = self.pool.alloc().ok_or(FdsFullError)?;
        io.reset(fd);
        io.register(direction);
        cached.set(Some(index));
        Ok((index, io))
    }

    fn get(&self, index: IoIndex) -> Option<&IoWaiter> {
        let (current, io) = self.pool.get(index.idx);
        (current == index).then_some(io)
    }

    pub(crate) fn release(&self, index: IoIndex, direction: Direction) -> Option<IoChange> {
        let io = self.get(index)?;
        let change = io.release(index, direction);
        if !io.is_registered() {
            self.pool.free(index);
        }
        change
    }

    pub(crate) fn wake_ready(&self, index: IoIndex, ready: Directions) {
        if let Some(io) = self.get(index) {
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
        io::{FdsFullError, IoIndex, IoWaiters},
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
        IoChange {
            token: index.pack(),
            fd,
            interest,
        }
    }

    #[test]
    fn register_wait_release() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (index, io) = io_waiters.register(cached, 3, Direction::Write).unwrap();
        assert!(!io.waker(Direction::Write).is_set());
        io.waker(Direction::Write).store(Waker::noop());
        assert!(io.waker(Direction::Write).is_set());
        assert_eq!(io_waiters.release(index, Direction::Write), None);
        assert!(!io.waker(Direction::Write).is_set());
        assert_eq!(io_waiters.count(), 0);
        let (reused, _) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        assert_ne!(reused, index);
    }

    #[test]
    fn both_directions_of_one_fd_share_an_entry() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (reader, _) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        let (writer, _) = io_waiters.register(cached, 3, Direction::Write).unwrap();
        assert_eq!(reader, writer);
        assert_eq!(io_waiters.count(), 1);
    }

    #[test]
    fn a_cached_index_of_another_fd_is_not_reused() {
        let io_waiters = &IoWaiters::<2>::new();
        let cached = &Cell::new(None);
        let (first, _) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        let (second, _) = io_waiters.register(cached, 4, Direction::Read).unwrap();
        assert_ne!(first, second);
        assert_eq!(cached.get(), Some(second));
    }

    #[test]
    fn registering_a_registered_direction_takes_it_over() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (leaked, io) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        let (index, io) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        assert_eq!(index, leaked);
        assert!(!io.waker(Direction::Read).is_set());
        assert_eq!(io_waiters.release(index, Direction::Read), None);
        assert_eq!(io_waiters.count(), 0);
    }

    #[test]
    fn changes_follow_the_stored_wakers() {
        let io_waiters = &IoWaiters::<1>::new();
        let (index, io) = io_waiters
            .register(&Cell::new(None), 3, Direction::Read)
            .unwrap();
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
        let (index, io) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        let _ = io_waiters.register(cached, 3, Direction::Write).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        io.waker(Direction::Write).store(Waker::noop());
        assert_pending_changes(io_waiters, [change(index, 3, Directions::Both)]);
    }

    #[test]
    fn releasing_an_armed_direction_returns_the_remaining_interest() {
        let io_waiters = &IoWaiters::<1>::new();
        let cached = &Cell::new(None);
        let (index, io) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        let _ = io_waiters.register(cached, 3, Direction::Write).unwrap();
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
        let (index, io) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        let _ = io_waiters.register(cached, 3, Direction::Write).unwrap();
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
        let (stale, _) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        io_waiters.release(stale, Direction::Read);
        let (_, io) = io_waiters.register(cached, 3, Direction::Read).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        io_waiters.mark_flushed();

        io_waiters.wake_ready(stale, Directions::Both);
        assert!(io.waker(Direction::Read).is_set());
        assert_pending_changes(io_waiters, []);
    }

    #[test]
    fn capacity_is_bounded() {
        let io_waiters = &IoWaiters::<1>::new();
        let _first = io_waiters
            .register(&Cell::new(None), 3, Direction::Read)
            .unwrap();
        assert_eq!(
            io_waiters
                .register(&Cell::new(None), 4, Direction::Read)
                .err(),
            Some(FdsFullError)
        );
    }
}
