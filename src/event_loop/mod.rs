mod error;

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

pub use self::error::Error;

use crate::{
    context::Context,
    selector::Selector,
    storage::LoopStorage,
    tasks::{Root, RootStatus},
};

/// The main type, an event loop.
#[must_use]
pub struct EventLoop<
    'a,
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    pub(crate) ctx: Context<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
}

impl<
    'a,
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> EventLoop<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    /// Creates an event loop and polls given future once.
    ///
    /// The future must fit `ROOT_TASK_SIZE` bytes and be aligned to at most 16 bytes, otherwise
    /// the program doesn't compile.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// 1. `storage` has already been used by another event loop, `f` isn't called then
    /// 2. epoll/kqueue returned an error during initial poll
    /// 3. given future completed during initial poll.
    pub fn new<F, Fut>(
        storage: &'static LoopStorage<E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        selector: &'a mut Selector,
        f: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(Context<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let ctx = storage.claim(selector).ok_or(Error::StorageAlreadyUsed)?;
        let event_loop = Self { ctx };
        ctx.storage.tasks.spawn_root(ctx.claimed, f(ctx));
        match ctx.storage.tasks.run_ready() {
            RootStatus::Running => {}
            RootStatus::Finished => return Err(Error::RootFinished),
        }
        ctx.flush()?;
        Ok(event_loop)
    }

    /// Drains all events from the underlying `epollfd` or `kqueuefd`,
    /// polls woken futures until none is ready,
    /// submits them back to epoll/kqueue.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// 1. epoll/kqueue returned an error
    /// 2. root future completed
    pub fn drain_events_without_blocking(&mut self) -> Result<(), Error> {
        let ctx = self.ctx;
        if matches!(ctx.storage.tasks.root(), Root::Finished) {
            return Err(Error::RootFinished);
        }
        loop {
            ctx.process_selector_events()?;
            if !ctx.storage.tasks.has_ready() {
                break;
            }
            if ctx.storage.tasks.run_ready() == RootStatus::Finished {
                ctx.storage.tasks.drop_all();
                ctx.flush()?;
                return Err(Error::RootFinished);
            }
        }
        ctx.flush()?;
        Ok(())
    }

    /// Makes the event loop's file descriptor readable and wakes up a task
    /// that waits on [`Context::notified`](crate::Context::notified)
    /// once you call [`drain_events_without_blocking`](Self::drain_events_without_blocking).
    ///
    /// This is the way to pass data from the external world into the event loop:
    /// store the data somewhere the task can reach it, then call this method.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails.
    pub fn notify(&self) -> rustix::io::Result<()> {
        self.ctx.selector.notify()
    }

    /// Returns an iterator that pops events pushed by [`Context::dispatch`](crate::Context::dispatch).
    ///
    /// This is the way to pass data from the event loop to the external world.
    pub fn events(&self) -> impl Iterator<Item = E> {
        self.ctx.storage.events.iter()
    }
}

impl<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Drop for EventLoop<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn drop(&mut self) {
        self.ctx.clear();
    }
}

impl<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> AsFd for EventLoop<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.ctx.selector.as_fd()
    }
}

impl<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> AsRawFd for EventLoop<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn as_raw_fd(&self) -> RawFd {
        self.as_fd().as_raw_fd()
    }
}

impl<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for EventLoop<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("EventLoop")
    }
}

#[cfg(test)]
mod tests;
