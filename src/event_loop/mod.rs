use core::{alloc::Layout, mem::MaybeUninit, ptr::NonNull};

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use crate::{
    capacities::Capacities,
    context::{Context, task_future},
    selectors::Selector,
    state::State,
    storage::{Root, RootStatus, Storage},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Io(rustix::io::Errno),
    RootFinished,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "event loop IO error: {errno}"),
            Self::RootFinished => f.write_str("the root future has finished"),
        }
    }
}

// No `source()`: without its `std` feature, rustix's `Errno` doesn't implement `Error`.
impl core::error::Error for Error {}

pub struct EventLoop<S: State> {
    pub(crate) storage: &'static Storage<S>,
}

impl<S: State> EventLoop<S> {
    pub fn required_size<F>(capacities: Capacities, _root: &F) -> usize
    where
        F: AsyncFnOnce(&Context<S>) + 'static,
    {
        Storage::<S>::required_size(capacities, return_layout(task_future::<S, F>))
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
        let event_loop = Self { storage };
        // SAFETY: the future is stored in this storage right away, as its root.
        let root = unsafe { task_future::<S, F>(storage, f) };
        storage.tasks.spawn_root(root);
        match storage.tasks.run_ready() {
            RootStatus::Running => {}
            RootStatus::Finished => return Err(Error::RootFinished),
        }
        event_loop.finish_drain(RootStatus::Running)?;
        Ok(event_loop)
    }

    pub fn drain_events_without_blocking(&mut self) -> Result<(), Error> {
        let storage = self.storage;
        if let Root::Finished = storage.tasks.root() {
            return Err(Error::RootFinished);
        }
        let status = loop {
            storage.process_selector_events().map_err(Error::Io)?;
            if !storage.tasks.has_ready() {
                break RootStatus::Running;
            }
            if storage.tasks.run_ready() == RootStatus::Finished {
                break RootStatus::Finished;
            }
        };
        self.finish_drain(status)
    }

    pub fn state_mut(&mut self) -> &mut S {
        // SAFETY: the state is only accessed mutably here and in `Context::dispatch`, which runs
        // inside tasks, i.e. only while `new`, a drain or `drop` holds exclusive access to the
        // loop; `&mut self` rules all of those out.
        unsafe { &mut *self.storage.state.get() }
    }

    fn finish_drain(&self, status: RootStatus) -> Result<(), Error> {
        if status == RootStatus::Finished {
            self.storage.tasks.drop_all();
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
        // SAFETY: destroyed exactly once, here, with no task being polled (`drop` can't run from
        // inside the loop).
        unsafe { self.storage.destroy() };
    }
}

impl<S: State> AsFd for EventLoop<S> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.storage.selector.as_fd()
    }
}

impl<S: State> AsRawFd for EventLoop<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.as_fd().as_raw_fd()
    }
}

#[cfg(test)]
mod tests;
