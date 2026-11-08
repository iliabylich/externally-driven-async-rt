use core::mem::MaybeUninit;

use rustix::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};

use crate::{
    context::Context,
    ready::ReadyQueue,
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
    'a,
    S: State,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> {
    pub(crate) storage: &'a Storage<
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
    pub const QUEUE_REQUIRED_SIZE: usize =
        size_of::<ReadyQueue<MAX_TASKS_COUNT>>() + align_of::<ReadyQueue<MAX_TASKS_COUNT>>() - 1;

    pub const REQUIRED_SIZE: usize = size_of::<
        Storage<
            'a,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
    >() + align_of::<
        Storage<
            'a,
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
    >() - 1;

    pub fn new<const QUEUE: usize, const MEMORY: usize, F>(
        queue: &'static mut [MaybeUninit<u8>; QUEUE],
        memory: &'a mut [MaybeUninit<u8>; MEMORY],
        state: S,
        f: F,
    ) -> Result<Self, Error>
    where
        F: AsyncFnOnce(
                &Context<
                    'a,
                    S,
                    MAX_TASKS_COUNT,
                    MAX_TASK_SIZE,
                    MAX_ROOT_TASK_SIZE,
                    MAX_TIMERS_COUNT,
                    MAX_IO_COUNT,
                >,
            ) + 'a,
    {
        // SAFETY: `queue` is never freed, so wakers pointing into it stay valid forever.
        unsafe { Self::new_in(queue, memory, state, f) }
    }

    /// # Safety
    ///
    /// No `Waker` handed to a task may be woken after the loop is dropped (or forgotten), because
    /// wakers point into `queue`.
    pub unsafe fn new_in<const QUEUE: usize, const MEMORY: usize, F>(
        queue: &'a mut [MaybeUninit<u8>; QUEUE],
        memory: &'a mut [MaybeUninit<u8>; MEMORY],
        state: S,
        f: F,
    ) -> Result<Self, Error>
    where
        F: AsyncFnOnce(
                &Context<
                    'a,
                    S,
                    MAX_TASKS_COUNT,
                    MAX_TASK_SIZE,
                    MAX_ROOT_TASK_SIZE,
                    MAX_TIMERS_COUNT,
                    MAX_IO_COUNT,
                >,
            ) + 'a,
    {
        let selector = Selector::new().map_err(Error::Io)?;
        // SAFETY: the queue is only reached through the loop, which `'a` bounds, and through its
        // wakers, which the caller guarantees aren't woken once the loop is gone, so treating it
        // as never freed is fine.
        let ready = unsafe { &*core::ptr::from_ref(ReadyQueue::init(place(queue))) };
        let storage = Storage::init(place(memory), ready, selector, state);
        let event_loop = Self { storage };
        Context::spawn_root(storage, f);
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
        // SAFETY: this loop is the only one using the storage, and `drop` runs at most once, so
        // the state and the selector are dropped exactly once, and never used again. No task (so
        // no `Context` and no reference into the storage) is left after `clear`; the state was
        // initialized in `new`, and the storage's other parts hold nothing that needs dropping
        // (the ready queue holds only `Copy` data, the blocks no live future, the pools nothing
        // after `clear`). The selector is moved out (a read) rather than dropped in place,
        // because dropping needs write access to the storage, which only shared references (like
        // `self.storage`) point to.
        unsafe {
            core::ptr::drop_in_place(self.storage.state.get());
            drop(core::ptr::read(&self.storage.selector));
        }
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
        self.storage.selector.as_fd()
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

fn place<T, const SIZE: usize>(memory: &mut [MaybeUninit<u8>; SIZE]) -> &mut MaybeUninit<T> {
    const {
        assert!(
            SIZE >= size_of::<T>() + align_of::<T>() - 1,
            "an EventLoop memory is smaller than its REQUIRED_SIZE (or QUEUE_REQUIRED_SIZE)"
        );
    }
    let addr = memory.as_ptr().addr();
    let start = addr.next_multiple_of(align_of::<T>()) - addr;
    // SAFETY: `start` is less than `T`'s alignment, so a `T` fits in `memory` from there (asserted
    // above), aligned. `MaybeUninit` needs no initialized contents, and `memory` is borrowed
    // exclusively for as long as the result.
    unsafe { &mut *memory.as_mut_ptr().add(start).cast() }
}

#[cfg(test)]
mod tests;
