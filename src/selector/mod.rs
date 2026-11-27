mod direction;
#[cfg(miri)]
mod dummy;
#[cfg(all(any(target_os = "linux", target_os = "android"), not(miri)))]
mod epoll;
mod kernel_timer;
#[cfg(all(target_os = "macos", not(miri)))]
mod kqueue;
mod selector_event;
mod timer_change;

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "android")))]
compile_error!("externally-driven-async-rt supports only macOS, Linux and Android");

use core::time::Duration;

use rustix::fd::{AsFd, BorrowedFd};

#[cfg(miri)]
use self::dummy::DummySelector as Backend;
#[cfg(all(any(target_os = "linux", target_os = "android"), not(miri)))]
use self::epoll::EpollSelector as Backend;
#[cfg(all(target_os = "macos", not(miri)))]
use self::kqueue::KqueueSelector as Backend;
pub(crate) use self::{
    direction::{Direction, Directions},
    kernel_timer::KernelTimer,
    selector_event::{EVENTS_PER_CALL, SelectorEvent},
    timer_change::TimerChange,
};
use crate::time::Instant;

/// A selector for your current platform (epoll on Linux and kqueue on macOS).
#[must_use]
pub struct Selector {
    backend: Backend,
}

impl Selector {
    /// Constructor.
    ///
    /// # Errors
    ///
    /// Returns an error if an epoll/kqueue syscall fails.
    pub fn new() -> rustix::io::Result<Self> {
        Ok(Self {
            backend: Backend::new()?,
        })
    }

    pub(crate) fn register(&self, fd: BorrowedFd<'_>) -> rustix::io::Result<()> {
        self.backend.register(fd)
    }

    pub(crate) fn deregister(&self, fd: BorrowedFd<'_>) -> rustix::io::Result<()> {
        self.backend.deregister(fd)
    }

    pub(crate) fn notify(&self) -> rustix::io::Result<()> {
        self.backend.notify()
    }

    pub(crate) fn flush(&self, deadline: Option<Instant>, now: Instant) -> rustix::io::Result<()> {
        self.backend.flush(deadline, now)
    }

    pub(crate) fn poll(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
        timeout: Option<Duration>,
    ) -> rustix::io::Result<bool> {
        self.backend.poll(out, timeout)
    }

    pub(crate) fn as_fd(&self) -> BorrowedFd<'_> {
        self.backend.as_fd()
    }
}
