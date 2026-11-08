use core::alloc::Layout;
use core::fmt;
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::ptr::NonNull;

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use crate::capacities::Capacities;
use crate::context::{Context, task_future};
use crate::selectors::Selector;
use crate::state::State;
use crate::storage::{Root, RootStatus, Storage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Io(rustix::io::Errno),
    RootFinished,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "event loop IO error: {errno}"),
            Self::RootFinished => f.write_str("the root future has finished"),
        }
    }
}

// No `source()`: without its `std` feature, rustix's `Errno` doesn't implement `Error`.
impl core::error::Error for Error {}

pub struct EventLoop<S: State> {
    storage: Storage,
    _state: PhantomData<S>,
}

impl<S: State> EventLoop<S> {
    pub fn required_size<F>(capacities: Capacities, _root: &F) -> usize
    where
        F: AsyncFnOnce(&Context<S>) + 'static,
    {
        Storage::<Selector>::required_size::<S>(capacities, return_layout(task_future::<S, F>))
    }

    pub fn new<F>(
        memory: &'static mut [MaybeUninit<u8>],
        state: S,
        capacities: Capacities,
        f: F,
    ) -> Result<Self, Error>
    where
        F: AsyncFnOnce(&Context<S>) + 'static,
    {
        let selector = Selector::new().map_err(Error::Io)?;
        // SAFETY: `memory` is borrowed exclusively forever, so nothing else ever uses it.
        let storage = unsafe {
            Storage::new(
                NonNull::from(memory),
                selector,
                state,
                capacities,
                return_layout(task_future::<S, F>),
            )
        };
        let event_loop = Self {
            storage,
            _state: PhantomData,
        };
        // SAFETY: the future is stored in this storage right away, as its only root.
        let root = unsafe { storage.spawn_root(task_future::<S, F>(storage, f)) };
        match storage.run_ready_tasks(root) {
            RootStatus::Running => {}
            RootStatus::Finished => return Err(Error::RootFinished),
        }
        event_loop.finish_drain(RootStatus::Running)?;
        Ok(event_loop)
    }

    pub fn drain_events_without_blocking(&mut self) -> Result<(), Error> {
        let storage = self.storage;
        let Root::Running(root) = storage.root() else {
            return Err(Error::RootFinished);
        };
        let status = loop {
            storage.process_selector_events().map_err(Error::Io)?;
            if !storage.has_ready_tasks() {
                break RootStatus::Running;
            }
            if storage.run_ready_tasks(root) == RootStatus::Finished {
                break RootStatus::Finished;
            }
        };
        self.finish_drain(status)
    }

    pub fn state_mut(&mut self) -> &mut S {
        // SAFETY: the storage was created with `S`. The state is only accessed mutably here and
        // in `Context::dispatch`, which runs inside tasks, i.e. only while `new`, a drain or
        // `drop` holds exclusive access to the loop; `&mut self` rules all of those out.
        unsafe { self.storage.state::<S>().as_mut() }
    }

    #[cfg(test)]
    pub(crate) fn storage(&self) -> Storage {
        self.storage
    }

    fn finish_drain(&self, status: RootStatus) -> Result<(), Error> {
        if status == RootStatus::Finished {
            self.storage.set_root(Root::Finished);
            self.storage.drop_all_tasks();
        }
        self.storage.flush().map_err(Error::Io)?;
        match status {
            RootStatus::Running => Ok(()),
            RootStatus::Finished => Err(Error::RootFinished),
        }
    }
}

// The root future's type can't be named, and its layout is needed to allocate the storage before
// the future can be created (it captures the storage).
fn return_layout<A, B, R>(_: unsafe fn(A, B) -> R) -> Layout {
    Layout::new::<R>()
}

impl<S: State> Drop for EventLoop<S> {
    fn drop(&mut self) {
        // SAFETY: the storage was created with `S` and is destroyed exactly once, here, with no
        // task being polled (`drop` can't run from inside the loop).
        unsafe { self.storage.destroy::<S>() };
    }
}

impl<S: State> AsFd for EventLoop<S> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.storage.selector().as_fd()
    }
}

impl<S: State> AsRawFd for EventLoop<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.as_fd().as_raw_fd()
    }
}

#[cfg(test)]
mod tests;
