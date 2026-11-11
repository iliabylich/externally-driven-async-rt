mod error;

use core::cell::{RefCell, RefMut};

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

pub use self::error::Error;

use crate::{
    context::Context,
    selector::Selector,
    state::State,
    storage::LoopStorage,
    tasks::{Root, RootStatus},
};

/// The main type, an event loop.
pub struct EventLoop<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    ctx: Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> EventLoop<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    /// Creates an event loop and polls given future once.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// 1. `storage` has already been used by another event loop
    /// 2. the future is bigger than `ROOT_TASK_SIZE` bytes
    /// 3. the future is aligned to more than 16 bytes.
    pub fn new<F, Fut>(
        storage: &'static LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        selector: &'a mut Selector,
        state: &'a mut RefCell<S>,
        f: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let ctx = storage.claim(selector, state);
        let event_loop = Self { ctx };
        let root = f(ctx);
        // SAFETY: `Fut: 'a`, where `'a` is the invariant lifetime of the `Context` that
        // `storage.claim` just created, the only one this storage will ever have. `EventLoop` drops
        // every task before `'a` ends, so the root outlives every poll and drop of its task.
        unsafe { ctx.storage.tasks.spawn_root(root) };
        match ctx.storage.tasks.run_ready() {
            RootStatus::Running => {}
            RootStatus::Finished => return Err(Error::RootFinished),
        }
        event_loop.finish_drain(RootStatus::Running)?;
        Ok(event_loop)
    }

    /// Drains all events from the underlying `epollfd` or `kqueuefd`,
    /// polls woken futures until none is ready,
    /// submits them back to epoll/kqueue.
    pub fn drain_events_without_blocking(&mut self) -> Result<(), Error> {
        let ctx = self.ctx;
        if let Root::Finished = ctx.storage.tasks.root() {
            return Err(Error::RootFinished);
        }
        let status = loop {
            ctx.process_selector_events().map_err(Error::Io)?;
            if !ctx.storage.tasks.has_ready() {
                break RootStatus::Running;
            }
            if ctx.storage.tasks.run_ready() == RootStatus::Finished {
                break RootStatus::Finished;
            }
        };
        self.finish_drain(status)
    }

    /// Returns a mutable reference to the stored state.
    ///
    /// This is the only way to communicate with the external world.
    pub fn state_mut(&mut self) -> RefMut<'_, S> {
        self.ctx.state.borrow_mut()
    }

    fn finish_drain(&self, status: RootStatus) -> Result<(), Error> {
        if status == RootStatus::Finished {
            self.ctx.storage.tasks.drop_all();
        }
        self.ctx.flush().map_err(Error::Io)?;
        match status {
            RootStatus::Running => Ok(()),
            RootStatus::Finished => Err(Error::RootFinished),
        }
    }

    #[cfg(test)]
    pub(crate) fn ctx(&self) -> Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS> {
        self.ctx
    }
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Drop for EventLoop<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn drop(&mut self) {
        self.ctx.clear();
    }
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> AsFd for EventLoop<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.ctx.selector.as_fd()
    }
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> AsRawFd for EventLoop<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn as_raw_fd(&self) -> RawFd {
        self.as_fd().as_raw_fd()
    }
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for EventLoop<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("EventLoop")
    }
}

#[cfg(test)]
mod tests;
