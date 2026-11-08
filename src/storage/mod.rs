use core::{
    cell::{Cell, UnsafeCell},
    task::Waker,
};

use crate::{
    instant::Instant,
    selectors::{Direction, EVENTS_PER_CALL, Selector, SelectorEvent},
};

pub(crate) use handle::Storage;
pub(crate) use io_waiters::{IoIndex, IoWaiters};
pub(crate) use tasks::{Root, RootStatus, TaskId, Tasks};
pub(crate) use timers::{TimerIndex, Timers};

fn store_waker(cell: &Cell<Option<Waker>>, waker: &Waker) {
    let stored = match cell.take() {
        Some(mut stored) => {
            stored.clone_from(waker);
            stored
        }
        None => waker.clone(),
    };
    cell.set(Some(stored));
}

pub struct LoopStorage<
    S,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> {
    pub(crate) selector: UnsafeCell<Option<Selector>>,
    pub(crate) state: UnsafeCell<Option<S>>,
    pub(crate) timers: Timers<MAX_TIMERS_COUNT>,
    pub(crate) io_waiters: IoWaiters<MAX_IO_COUNT>,
    pub(crate) tasks: Tasks<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
}

// SAFETY: a storage has no public methods, so other threads can only pass `&LoopStorage` around.
// The crate touches it only after `claim`, which succeeds once ever, on the thread that runs the
// claiming loop; `EventLoop` and `Context` are `!Send`, so everything but the wakers stays on
// that thread, and wakers only touch the atomic `WakeQueue`, which asserts that thread.
unsafe impl<
    S,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Sync
    for LoopStorage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
}

impl<
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>
    LoopStorage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    #[expect(
        clippy::new_without_default,
        reason = "a storage lives in a `static`, which needs a `const fn`"
    )]
    pub const fn new() -> Self {
        Self {
            selector: UnsafeCell::new(None),
            state: UnsafeCell::new(None),
            timers: Timers::new(),
            io_waiters: IoWaiters::new(),
            tasks: Tasks::new(),
        }
    }

    pub(crate) fn claim<'a>(
        &'static self,
        selector: Option<Selector>,
        state: S,
    ) -> Storage<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    > {
        self.tasks.claim();
        // SAFETY: the claim above succeeds only once per storage, so nothing has accessed (or
        // holds a reference to) the selector or the state yet.
        unsafe {
            *self.selector.get() = selector;
            *self.state.get() = Some(state);
        }
        Storage::new(self)
    }

    pub(crate) fn selector(&self) -> &Selector {
        // SAFETY: the selector is written only in `claim`, before any reference to it can exist,
        // and taken only in `EventLoop`'s `drop`, after the tasks (which hold the only other
        // references, besides the loop's own borrows) are gone.
        unsafe { &*self.selector.get() }
            .as_ref()
            .expect("the storage is claimed by an EventLoop")
    }

    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector().poll_nonblocking(&mut events)?;
            for event in events.iter().map_while(|event| *event) {
                match event {
                    SelectorEvent::Readable(token) => {
                        if let Some(waker) = self.io_waiters.take_waker(token, Direction::Read) {
                            waker.wake();
                        }
                    }
                    SelectorEvent::Writable(token) => {
                        if let Some(waker) = self.io_waiters.take_waker(token, Direction::Write) {
                            waker.wake();
                        }
                    }
                    SelectorEvent::Timer => {
                        let now = Instant::now();
                        while let Some(waker) = self.timers.take_expired(now) {
                            waker.wake();
                        }
                    }
                }
            }
            if !full {
                return Ok(());
            }
        }
    }

    pub(crate) fn flush(&self) -> rustix::io::Result<()> {
        let selector = self.selector();
        match self.timers.next_deadline() {
            Some(deadline) => selector.arm_timer(deadline),
            None => selector.disarm_timer(),
        }
        selector.flush(self.io_waiters.pending_changes())?;
        self.io_waiters.mark_flushed();
        Ok(())
    }
}

mod handle;
mod io_waiters;
mod tasks;
mod timers;

#[cfg(test)]
mod tests;
