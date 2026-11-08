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
