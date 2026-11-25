mod error;

use core::{mem::DropGuard, task::Poll};

use rustix::fd::{AsFd, BorrowedFd};

pub use self::error::EventLoopError;

use crate::{context::Context, selector::Selector, storage::LoopStorage};

/// The main type, an event loop.
#[must_use]
pub struct EventLoop<
    'sel,
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    pub(crate) ctx: Context<'sel, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    poisoned: bool,
}

impl<
    'sel,
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> EventLoop<'sel, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
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
        selector: &'sel mut Selector,
        f: F,
    ) -> Result<Self, EventLoopError>
    where
        F: FnOnce(Context<'sel, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>) -> Fut,
        Fut: Future<Output = ()> + 'sel,
    {
        let (ctx, root) = storage.claim(selector).ok_or(EventLoopError::StorageAlreadyUsed)?;
        let event_loop = Self {
            ctx,
            poisoned: false,
        };
        ctx.storage.tasks.spawn_root(root, ctx.claimed, f(ctx));
        match ctx.storage.tasks.run_ready() {
            Poll::Pending => {}
            Poll::Ready(()) => return Err(EventLoopError::RootFinished),
        }
        ctx.flush()?;
        Ok(event_loop)
    }

    /// Drains all events from the underlying `epollfd` or `kqueuefd`,
    /// polls woken futures until none is ready,
    /// submits them back to epoll/kqueue.
    ///
    /// It keeps polling until no future is woken, so a future that wakes itself
    /// every time it's polled makes this method loop forever.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// 1. epoll/kqueue returned an error
    /// 2. root future completed
    /// 3. a previous call panicked, the event loop is poisoned then
    pub fn drain_events_without_blocking(&mut self) -> Result<(), EventLoopError> {
        if self.poisoned {
            return Err(EventLoopError::Poisoned);
        }
        let ctx = self.ctx;
        let poison = DropGuard::new(&mut self.poisoned, |poisoned: &mut bool| *poisoned = true);
        let result = ctx.drain();
        DropGuard::dismiss(poison);
        result
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
        self.ctx.storage.events.iter_removing()
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

#[cfg(test)]
mod tests;
