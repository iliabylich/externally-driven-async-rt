use core::{
    cell::{Cell, UnsafeCell},
    mem::MaybeUninit,
    task::Waker,
};

use crate::{
    instant::Instant,
    selectors::{Direction, EVENTS_PER_CALL, Selector, SelectorEvent},
};

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

pub(crate) struct Storage<
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    Sel = Selector,
> {
    pub(crate) selector: Sel,
    pub(crate) state: UnsafeCell<S>,
    pub(crate) timers: Timers<MAX_TIMERS_COUNT>,
    pub(crate) io_waiters: IoWaiters<MAX_IO_COUNT>,
    pub(crate) tasks: Tasks<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
}

impl<
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    Sel: 'static,
>
    Storage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Sel,
    >
{
    pub(crate) fn init(place: &mut MaybeUninit<Self>, selector: Sel, state: S) -> &Self {
        let ptr = place.as_mut_ptr();
        // SAFETY: `ptr` points to `place`, borrowed exclusively until the end; the fields are
        // disjoint, `&raw mut` creates no reference to them while they're uninitialized, and
        // `MaybeUninit<T>` has `T`'s layout.
        let (selector_place, state_place, timers, io_waiters, tasks) = unsafe {
            (
                &mut *(&raw mut (*ptr).selector).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).state).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).timers).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).io_waiters).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).tasks).cast::<MaybeUninit<_>>(),
            )
        };
        selector_place.write(selector);
        state_place.write(UnsafeCell::new(state));
        Timers::<MAX_TIMERS_COUNT>::init(timers);
        IoWaiters::<MAX_IO_COUNT>::init(io_waiters);
        Tasks::<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>::init(tasks);
        // SAFETY: every field was written above.
        unsafe { place.assume_init_ref() }
    }

    /// # Safety
    ///
    /// Called exactly once, while no task is being polled; the storage isn't used afterwards.
    pub(crate) unsafe fn destroy(&self) {
        self.tasks.drop_all();
        self.timers.clear();
        self.io_waiters.clear();
        // SAFETY: no task (so no `Context` and no reference into the storage) is left; the state
        // was initialized in `init` and is dropped exactly once here, the other parts hold nothing
        // that needs dropping (the ready queue holds only `Copy` data, the blocks no live future,
        // the pools nothing after `drop_all` and `clear`). The selector is
        // this struct's only field with drop glue; it's moved out (a read) rather than dropped in
        // place, because dropping needs write access to the struct, which callers hold protected
        // shared references to (`&self`, and e.g. an `EventLoop` given to `mem::drop`).
        unsafe {
            core::ptr::drop_in_place(self.state.get());
            drop(core::ptr::read(&self.selector));
        }
    }
}

impl<
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Storage<S, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE, MAX_TIMERS_COUNT, MAX_IO_COUNT>
{
    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector.poll_nonblocking(&mut events)?;
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
        match self.timers.next_deadline() {
            Some(deadline) => self.selector.arm_timer(deadline),
            None => self.selector.disarm_timer(),
        }
        self.selector.flush(self.io_waiters.pending_changes())?;
        self.io_waiters.mark_flushed();
        Ok(())
    }
}

mod io_waiters;
mod tasks;
mod timers;

#[cfg(test)]
mod tests;
