use rustix::fd::RawFd;

use crate::{
    const_unreachable::const_unreachable,
    data::fd_hash_map::FdHashMap,
    io::{FdsFullError, IoWaiter},
    selector::{Direction, Directions, IoChange},
};

#[must_use]
pub(crate) struct IoWaiters<const N: usize> {
    map: FdHashMap<IoWaiter, N>,
}

impl<const N: usize> IoWaiters<N> {
    pub(crate) const fn new() -> Self {
        Self {
            map: FdHashMap::new([const { IoWaiter::new() }; N]),
        }
    }

    pub(crate) fn register(
        &self,
        fd: RawFd,
        direction: Direction,
    ) -> Result<&IoWaiter, FdsFullError> {
        let io = self.map.get_or_insert(fd).ok_or(FdsFullError)?;
        io.register(direction);
        Ok(io)
    }

    pub(crate) fn release(&self, fd: RawFd, direction: Direction) -> Option<IoChange> {
        let Some(io) = self.map.get(fd) else {
            const_unreachable!("releasing fd {} that is not registered", fd);
        };
        let change = io.release(fd, direction);
        if !io.is_registered() {
            self.map.remove(fd);
        }
        change
    }

    pub(crate) fn wake_ready(&self, fd: RawFd, ready: Directions) {
        if let Some(io) = self.map.get(fd) {
            io.wake(ready);
        }
    }

    pub(crate) fn pending_changes(&self) -> impl Iterator<Item = IoChange> + '_ {
        self.iter().filter_map(|(fd, io)| io.pending_change(fd))
    }

    pub(crate) fn mark_flushed(&self) {
        for (_, io) in self.iter() {
            io.mark_flushed();
        }
    }

    pub(crate) fn drop_wakers(&self) {
        for (_, io) in self.iter() {
            io.drop_wakers();
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (RawFd, &IoWaiter)> {
        self.map.iter()
    }
}

#[cfg(test)]
mod tests {
    use core::task::Waker;

    use crate::{
        io::{FdsFullError, IoWaiters},
        selector::{Direction, Directions, IoChange},
        testing::assert_yields,
    };

    fn change(fd: i32, interest: Directions) -> IoChange {
        IoChange { fd, interest }
    }

    #[test]
    fn register_wait_release() {
        let io_waiters = IoWaiters::<1>::new();
        let io = io_waiters.register(3, Direction::Write).unwrap();
        assert!(!io.waker(Direction::Write).is_set());
        io.waker(Direction::Write).store(Waker::noop());
        assert!(io.waker(Direction::Write).is_set());
        assert_eq!(io_waiters.release(3, Direction::Write), None);
        assert!(!io.waker(Direction::Write).is_set());
        assert_eq!(io_waiters.iter().count(), 0);
        io_waiters.register(3, Direction::Read).unwrap();
        assert_eq!(io_waiters.iter().count(), 1);
    }

    #[test]
    fn registering_a_registered_direction_takes_it_over() {
        let io_waiters = IoWaiters::<1>::new();
        let io = io_waiters.register(3, Direction::Read).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        let io = io_waiters.register(3, Direction::Read).unwrap();
        assert!(!io.waker(Direction::Read).is_set());
        assert_eq!(io_waiters.release(3, Direction::Read), None);
        assert_eq!(io_waiters.iter().count(), 0);
    }

    #[test]
    fn changes_follow_the_stored_wakers() {
        let io_waiters = IoWaiters::<1>::new();
        let io = io_waiters.register(3, Direction::Read).unwrap();
        assert_yields!(io_waiters.pending_changes(), []);

        io.waker(Direction::Read).store(Waker::noop());
        assert_yields!(io_waiters.pending_changes(), [change(3, Directions::Read)]);
        io_waiters.mark_flushed();
        assert_yields!(io_waiters.pending_changes(), []);

        io.waker(Direction::Read).take();
        assert_yields!(io_waiters.pending_changes(), [change(3, Directions::Empty)]);
    }

    #[test]
    fn one_change_per_fd_with_the_combined_interest() {
        let io_waiters = IoWaiters::<1>::new();
        let io = io_waiters.register(3, Direction::Read).unwrap();
        io_waiters.register(3, Direction::Write).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        io.waker(Direction::Write).store(Waker::noop());
        assert_yields!(io_waiters.pending_changes(), [change(3, Directions::Both)]);
    }

    #[test]
    fn releasing_an_armed_direction_returns_the_remaining_interest() {
        let io_waiters = IoWaiters::<1>::new();
        let io = io_waiters.register(3, Direction::Read).unwrap();
        io_waiters.register(3, Direction::Write).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        io.waker(Direction::Write).store(Waker::noop());
        io_waiters.mark_flushed();

        assert_eq!(
            io_waiters.release(3, Direction::Read),
            Some(change(3, Directions::Write))
        );
        assert_eq!(io_waiters.release(3, Direction::Read), None);
        assert_eq!(io_waiters.iter().count(), 1);
        assert_eq!(
            io_waiters.release(3, Direction::Write),
            Some(change(3, Directions::Empty))
        );
        assert_eq!(io_waiters.iter().count(), 0);
    }

    #[test]
    fn an_event_wakes_only_the_ready_direction_and_rearms_the_other() {
        let io_waiters = IoWaiters::<1>::new();
        let io = io_waiters.register(3, Direction::Read).unwrap();
        io_waiters.register(3, Direction::Write).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        io.waker(Direction::Write).store(Waker::noop());
        io_waiters.mark_flushed();

        io_waiters.wake_ready(3, Directions::Read);
        assert!(!io.waker(Direction::Read).is_set());
        assert!(io.waker(Direction::Write).is_set());
        assert_yields!(io_waiters.pending_changes(), [change(3, Directions::Write)]);
    }

    #[test]
    fn an_event_for_an_unregistered_fd_is_ignored() {
        let io_waiters = IoWaiters::<1>::new();
        let io = io_waiters.register(3, Direction::Read).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        io_waiters.mark_flushed();

        io_waiters.wake_ready(4, Directions::Both);
        assert!(io.waker(Direction::Read).is_set());
        assert_yields!(io_waiters.pending_changes(), []);
    }

    #[test]
    fn capacity_is_bounded() {
        let io_waiters = IoWaiters::<1>::new();
        io_waiters.register(3, Direction::Read).unwrap();
        assert_eq!(
            io_waiters.register(4, Direction::Read).err(),
            Some(FdsFullError)
        );
    }

    #[test]
    fn releasing_after_dropping_wakers_removes_the_fd() {
        let io_waiters = IoWaiters::<1>::new();
        let io = io_waiters.register(3, Direction::Read).unwrap();
        io_waiters.register(3, Direction::Write).unwrap();
        io.waker(Direction::Read).store(Waker::noop());
        io.waker(Direction::Write).store(Waker::noop());
        io_waiters.drop_wakers();
        assert!(!io.waker(Direction::Read).is_set());
        assert!(!io.waker(Direction::Write).is_set());
        assert_eq!(io_waiters.release(3, Direction::Read), None);
        assert_eq!(io_waiters.release(3, Direction::Write), None);
        assert_eq!(io_waiters.iter().count(), 0);
    }
}
