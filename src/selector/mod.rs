mod direction;
#[cfg(miri)]
mod dummy;
#[cfg(all(any(target_os = "linux", target_os = "android"), not(miri)))]
mod epoll;
mod io_change;
mod kernel_timer;
#[cfg(all(target_os = "macos", not(miri)))]
mod kqueue;
mod selector_event;
mod timer_change;

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "android")))]
compile_error!("externally-driven-async-rt supports only macOS, Linux and Android");

use rustix::fd::{AsFd, BorrowedFd};

#[cfg(miri)]
use self::dummy::DummySelector as Backend;
#[cfg(all(any(target_os = "linux", target_os = "android"), not(miri)))]
use self::epoll::EpollSelector as Backend;
#[cfg(all(target_os = "macos", not(miri)))]
use self::kqueue::KqueueSelector as Backend;
pub(crate) use self::{
    direction::{Direction, Directions},
    io_change::IoChange,
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

    pub(crate) fn apply_now(&self, change: IoChange) -> rustix::io::Result<()> {
        self.backend.apply_now(change)
    }

    pub(crate) fn arm_timer(&self, deadline: Instant) {
        self.backend.arm_timer(deadline);
    }

    pub(crate) fn disarm_timer(&self) {
        self.backend.disarm_timer();
    }

    pub(crate) fn notify(&self) -> rustix::io::Result<()> {
        self.backend.notify()
    }

    pub(crate) fn flush(
        &self,
        io_changes: impl IntoIterator<Item = IoChange>,
    ) -> rustix::io::Result<()> {
        self.backend.flush(io_changes)
    }

    pub(crate) fn poll_nonblocking(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
    ) -> rustix::io::Result<bool> {
        self.backend.poll_nonblocking(out)
    }

    pub(crate) fn as_fd(&self) -> BorrowedFd<'_> {
        self.backend.as_fd()
    }
}

impl core::fmt::Debug for Selector {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Selector")
    }
}

#[cfg(test)]
mod tests;
