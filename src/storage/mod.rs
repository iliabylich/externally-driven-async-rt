use core::{
    cell::{Cell, RefCell},
    task::Waker,
};

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
> {
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
            state: RefCell::new(None),
            timers: Timers::new(),
            io_waiters: IoWaiters::new(),
            tasks: Tasks::new(),
        }
    }

    pub(crate) fn claim<'a, Sel>(
        &'static self,
        selector: &'a Sel,
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
        *self.state.borrow_mut() = Some(state);
        LoopStorageGuard {
            storage: self,
            selector,
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
