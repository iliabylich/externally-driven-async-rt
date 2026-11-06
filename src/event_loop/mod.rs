use core::fmt;

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use crate::context::{Context, task_future};
use crate::inner::{OwnedInner, RootStatus};
use crate::state::State;
use crate::task::TaskId;

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

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Io(errno) => Some(errno),
            Self::RootFinished => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Root {
    Running(TaskId),
    Finished,
}

pub struct EventLoop<S: State> {
    inner: OwnedInner<S::Event>,
    state: S,
    root: Root,
}

impl<S: State> EventLoop<S> {
    pub fn new<F>(state: S, f: F) -> Result<Self, Error>
    where
        F: AsyncFnOnce(&Context<S::Event>) + 'static,
    {
        let inner = OwnedInner::new().map_err(Error::Io)?;
        // SAFETY: the future is stored in this `Inner`'s task storage right away.
        let root = inner.get().spawn(unsafe { task_future(inner.as_ptr(), f) });
        let mut event_loop = Self {
            inner,
            state,
            root: Root::Running(root),
        };
        match event_loop.inner.get().run_ready_tasks(root) {
            RootStatus::Running => {}
            RootStatus::Finished => return Err(Error::RootFinished),
        }
        event_loop.finish_drain(RootStatus::Running)?;
        Ok(event_loop)
    }

    pub fn drain_events_without_blocking(&mut self) -> Result<(), Error> {
        let Root::Running(root) = self.root else {
            return Err(Error::RootFinished);
        };
        let inner = self.inner.get();
        let status = loop {
            inner.process_selector_events().map_err(Error::Io)?;
            if !inner.has_ready_tasks() {
                break RootStatus::Running;
            }
            if inner.run_ready_tasks(root) == RootStatus::Finished {
                break RootStatus::Finished;
            }
        };
        self.finish_drain(status)
    }

    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }

    #[cfg(test)]
    pub(crate) fn inner(&self) -> &crate::inner::Inner<S::Event> {
        self.inner.get()
    }

    fn finish_drain(&mut self, status: RootStatus) -> Result<(), Error> {
        let inner = self.inner.get();
        if status == RootStatus::Finished {
            self.root = Root::Finished;
            inner.drop_all_tasks();
        }
        while let Some(event) = inner.pop_event() {
            self.state.apply(event);
        }
        inner.flush().map_err(Error::Io)?;
        match status {
            RootStatus::Running => Ok(()),
            RootStatus::Finished => Err(Error::RootFinished),
        }
    }
}

impl<S: State> AsFd for EventLoop<S> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.get().as_fd()
    }
}

impl<S: State> AsRawFd for EventLoop<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.as_fd().as_raw_fd()
    }
}

#[cfg(test)]
mod tests;
