use core::{alloc::Layout, cell::Cell, ptr::NonNull, task::Waker};

use rustix::fd::RawFd;

use crate::{
    pool::{Index, ObjectPool},
    selectors::{Direction, IoAction, IoChange, Token},
};

// Pool values are `Cell`s (see `ObjectPool`); a vacant slot holds a stale or default value.
pub(crate) struct IoWaiter {
    fd: Cell<RawFd>,
    direction: Cell<Direction>,
    waker: Cell<Option<Waker>>,
    // Whether the kernel filter is currently armed; the desired state is `is_waiting()`.
    armed: Cell<bool>,
}

impl Default for IoWaiter {
    fn default() -> Self {
        Self {
            fd: Cell::new(-1),
            direction: Cell::new(Direction::Read),
            waker: Cell::new(None),
            armed: Cell::new(false),
        }
    }
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

// IO registrations, one per pending IO future (see `IoRegistration`), and the kernel interest
// they want.
pub(crate) struct IoWaiters {
    pool: &'static ObjectPool<IoWaiter>,
}

impl IoWaiters {
    pub(crate) fn layout(len: usize) -> Layout {
        ObjectPool::<IoWaiter>::layout(len)
    }

    /// # Safety
    ///
    /// As `ObjectPool::init`, with the memory valid (and otherwise unused) forever.
    pub(crate) unsafe fn init(ptr: NonNull<u8>, len: usize) -> Self {
        Self {
            // SAFETY: guaranteed by the caller.
            pool: unsafe { ObjectPool::init(ptr, len) },
        }
    }

    /// In memory only: kernel interest is armed by the next flush, once a waker is stored.
    pub(crate) fn register(&self, fd: RawFd, direction: Direction) -> IoIndex {
        let Some((index, io)) = self.pool.alloc() else {
            panic!(
                "more than {} concurrent IO operations",
                self.pool.capacity()
            );
        };
        io.fd.set(fd);
        io.direction.set(direction);
        io.waker.set(None);
        io.armed.set(false);
        index
    }

    /// Returns the change that deletes the registration's filter if it's still armed, which the
    /// caller must apply right away, while the fd is still open (see `Selector::apply_now`).
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

    /// For an event delivered for `token`: the filter is one-shot, so delivering it disarmed it.
    pub(crate) fn take_waker(&self, token: Token, direction: Direction) -> Option<Waker> {
        let io = self.pool.get(IoIndex::from_bits(token.0))?;
        if io.direction.get() != direction {
            return None;
        }
        io.armed.set(false);
        io.waker.take()
    }

    /// Every registration whose kernel interest differs from what it wants.
    pub(crate) fn pending_changes(&self) -> impl Iterator<Item = IoChange> + '_ {
        self.pool
            .iter()
            .filter_map(|(index, io)| io.pending_change(index))
    }

    /// After the pending changes were submitted: every registration's interest is what it wants.
    pub(crate) fn mark_flushed(&self) {
        for (_, io) in self.pool.iter() {
            io.armed.set(io.is_waiting());
        }
    }

    /// Releases every registration without touching the kernel, so the pool holds nothing that
    /// needs dropping.
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
    use core::{mem::MaybeUninit, task::Waker};

    use crate::{
        selectors::{Direction, IoAction, IoChange, Token},
        storage::{IoIndex, IoWaiters},
        test_utils::{aligned_in, assert_yields, stack_memory, waker},
    };

    // No cleanup needed: the tests' wakers do nothing when dropped.
    fn io_waiters_in(memory: &mut [MaybeUninit<u8>], len: usize) -> IoWaiters {
        let ptr = aligned_in(memory, IoWaiters::layout(len));
        // SAFETY: `ptr` is aligned and valid for the layout, and every test keeps `memory` alive
        // for as long as the returned registrations.
        unsafe { IoWaiters::init(ptr, len) }
    }

    #[track_caller]
    fn assert_pending_changes(
        io_waiters: &IoWaiters,
        expected: impl IntoIterator<Item = IoChange>,
    ) {
        assert_yields(io_waiters.pending_changes(), expected);
    }

    #[test]
    fn register_wait_release() {
        let mut memory = stack_memory();
        let io_waiters = io_waiters_in(&mut memory, 1);
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
        let mut memory = stack_memory();
        let io_waiters = io_waiters_in(&mut memory, 2);
        let reader = io_waiters.register(3, Direction::Read);
        let writer = io_waiters.register(4, Direction::Write);
        assert_pending_changes(&io_waiters, []);

        let change = |action| IoChange {
            token: Token(reader.to_bits()),
            fd: 3,
            direction: Direction::Read,
            action,
        };
        io_waiters.set_waker(reader, Waker::noop());
        assert_pending_changes(&io_waiters, [change(IoAction::Arm)]);
        io_waiters.mark_flushed();
        assert_pending_changes(&io_waiters, []);

        io_waiters.remove_waker(reader);
        assert_pending_changes(&io_waiters, [change(IoAction::Disarm)]);
        assert_eq!(IoIndex::from_bits(reader.to_bits()), reader);
        assert!(!io_waiters.has_waker(writer));
        assert_eq!(io_waiters.count(), 2);
    }

    #[test]
    fn releasing_an_armed_registration_returns_its_disarm() {
        let mut memory = stack_memory();
        let io_waiters = io_waiters_in(&mut memory, 1);
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
        let mut memory = stack_memory();
        let io_waiters = io_waiters_in(&mut memory, 1);
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
        let mut memory = stack_memory();
        let io_waiters = io_waiters_in(&mut memory, 1);
        io_waiters.register(3, Direction::Read);
        io_waiters.register(3, Direction::Write);
    }
}
