use core::{
    cell::{Cell, RefCell, UnsafeCell},
    task::Waker,
};

use crate::selectors::Selector;

pub(crate) use guard::LoopStorageGuard;
pub(crate) use io_waiters::{IoIndex, IoWaiters};
pub(crate) use tasks::{Root, RootStatus, TaskId, Tasks, TasksGuard};
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
    Sel = Selector,
> {
    selector: UnsafeCell<Option<Sel>>,
    state: RefCell<Option<S>>,
    timers: Timers<MAX_TIMERS_COUNT>,
    io_waiters: IoWaiters<MAX_IO_COUNT>,
    tasks: Tasks<MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
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
    Sel,
> Sync
    for LoopStorage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Sel,
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
    Sel: 'static,
>
    LoopStorage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Sel,
    >
{
    #[expect(
        clippy::new_without_default,
        reason = "a storage lives in a `static`, which needs a `const fn`"
    )]
    pub const fn new() -> Self {
        Self {
            selector: UnsafeCell::new(None),
            state: RefCell::new(None),
            timers: Timers::new(),
            io_waiters: IoWaiters::new(),
            tasks: Tasks::new(),
        }
    }

    pub(crate) fn claim<'a>(
        &'static self,
        selector: Sel,
        state: S,
    ) -> LoopStorageGuard<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Sel,
    > {
        let tasks_guard = self.tasks.claim();
        // SAFETY: the claim above succeeds only once per storage, so nothing has accessed (or
        // holds a reference to) the selector yet.
        unsafe { *self.selector.get() = Some(selector) };
        *self.state.borrow_mut() = Some(state);
        LoopStorageGuard {
            storage: self,
            tasks: tasks_guard,
        }
    }
}

mod guard;
mod io_waiters;
mod tasks;
mod timers;

#[cfg(test)]
mod tests;
