mod error;

use core::cell::RefMut;

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

pub use self::error::Error;

use crate::{
    context::Context,
    selector::Selector,
    state::State,
    storage::{LoopStorage, LoopStorageGuard},
    tasks::{Root, RootStatus},
};

/// The main type, an event loop.
pub struct EventLoop<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> {
    storage: LoopStorageGuard<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
}

impl<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>
    EventLoop<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    /// Creates an event loop and polls given future once.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// 1. `storage` has already been used by another event loop
    /// 2. the future is bigger than `MAX_ROOT_TASK_SIZE` bytes
    /// 3. the future is aligned to more than 16 bytes.
    pub fn new<F, Fut>(
        storage: &'static LoopStorage<
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        selector: &'a mut Selector,
        state: S,
        f: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(
            Context<
                'a,
                S,
                MAX_TASKS_COUNT,
                MAX_TASK_SIZE,
                MAX_ROOT_TASK_SIZE,
                MAX_TIMERS_COUNT,
                MAX_IO_COUNT,
            >,
        ) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let storage = storage.claim(selector, state);
        let event_loop = Self { storage };
        Context::spawn_root(storage, f);
        match storage.tasks().run_ready() {
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
        let storage = self.storage;
        if let Root::Finished = storage.tasks().root() {
            return Err(Error::RootFinished);
        }
        let status = loop {
            storage.process_selector_events().map_err(Error::Io)?;
            if !storage.tasks().has_ready() {
                break RootStatus::Running;
            }
            if storage.tasks().run_ready() == RootStatus::Finished {
                break RootStatus::Finished;
            }
        };
        self.finish_drain(status)
    }

    /// Returns a mutable reference to the stored state.
    ///
    /// This is the only way to communicate with the external world.
    pub fn state_mut(&mut self) -> RefMut<'_, S> {
        RefMut::map(self.storage.state_slot().borrow_mut(), |state| {
            state
                .as_mut()
                .expect("the state is set until the loop is dropped")
        })
    }

    fn finish_drain(&self, status: RootStatus) -> Result<(), Error> {
        if status == RootStatus::Finished {
            self.storage.tasks().drop_all();
        }
        self.storage.flush().map_err(Error::Io)?;
        match status {
            RootStatus::Running => Ok(()),
            RootStatus::Finished => Err(Error::RootFinished),
        }
    }

    #[cfg(test)]
    pub(crate) fn storage(
        &self,
    ) -> LoopStorageGuard<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    > {
        self.storage
    }
}

impl<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Drop
    for EventLoop<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn drop(&mut self) {
        self.storage.clear();
        drop(self.storage.state_slot().borrow_mut().take());
    }
}

impl<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> AsFd
    for EventLoop<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.storage.selector().as_fd()
    }
}

impl<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> AsRawFd
    for EventLoop<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn as_raw_fd(&self) -> RawFd {
        self.as_fd().as_raw_fd()
    }
}

impl<
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> core::fmt::Debug
    for EventLoop<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("EventLoop")
    }
}

#[cfg(test)]
mod tests;
