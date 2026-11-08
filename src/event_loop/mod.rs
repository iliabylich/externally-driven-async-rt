use core::mem::MaybeUninit;

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use crate::{
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

impl core::error::Error for Error {}

pub struct EventLoop<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> {
    pub(crate) storage: &'static Storage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
}

impl<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> EventLoop<S, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE, MAX_TIMERS_COUNT, MAX_IO_COUNT>
{
    pub const REQUIRED_SIZE: usize = size_of::<
        Storage<
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
    >() + align_of::<
        Storage<
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
    >() - 1;

    pub fn new<const MEMORY: usize, F>(
        memory: &'static mut [MaybeUninit<u8>; MEMORY],
        state: S,
        f: F,
    ) -> Result<Self, Error>
    where
        F: AsyncFnOnce(
                &Context<
                    S,
                    MAX_TASKS_COUNT,
                    MAX_TASK_SIZE,
                    MAX_ROOT_TASK_SIZE,
                    MAX_TIMERS_COUNT,
                    MAX_IO_COUNT,
                >,
            ) + 'static,
    {
        const {
            assert!(
                MEMORY >= Self::REQUIRED_SIZE,
                "the EventLoop memory is smaller than REQUIRED_SIZE"
            );
        }
        let selector = Selector::new().map_err(Error::Io)?;
        let align = align_of::<
            Storage<
                S,
                MAX_TASKS_COUNT,
                MAX_TASK_SIZE,
                MAX_ROOT_TASK_SIZE,
                MAX_TIMERS_COUNT,
                MAX_IO_COUNT,
            >,
        >();
        let addr = memory.as_ptr().addr();
        let start = addr.next_multiple_of(align) - addr;
        // SAFETY: `start` is less than `align`, so the storage fits in `memory` from there
        // (`REQUIRED_SIZE`, asserted above), aligned. `MaybeUninit` needs no initialized contents,
        // and `memory` is borrowed exclusively forever.
        let place = unsafe { &mut *memory.as_mut_ptr().add(start).cast() };
        let storage = Storage::init(place, selector, state);
        let event_loop = Self { storage };
        // SAFETY: the future is stored in this storage right away, as its root.
        let root = unsafe { task_future(storage, f) };
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

impl<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Drop
    for EventLoop<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn drop(&mut self) {
        // SAFETY: destroyed exactly once, here, with no task being polled (`drop` can't run from
        // inside the loop).
        unsafe { self.storage.destroy() };
    }
}

impl<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> AsFd
    for EventLoop<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.storage.selector.as_fd()
    }
}

impl<
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> AsRawFd
    for EventLoop<
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

#[cfg(test)]
mod tests;
