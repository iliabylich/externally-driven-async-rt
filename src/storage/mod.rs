use core::cell::RefCell;

pub(crate) use self::guard::LoopStorageGuard;
use crate::{io::IoWaiters, tasks::Tasks, time::Timers};

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
// The crate touches it only through a `LoopStorageGuard`, which only `claim` creates, and `claim`
// succeeds once ever, on the thread that runs the claiming loop; `EventLoop` and `Context` are
// `!Send`, so everything but the wakers stays on that thread, and wakers only touch the atomic
// `WakeQueue`, which asserts that thread.
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
        LoopStorageGuard::new(self, selector, tasks_guard)
    }
}

mod guard;

#[cfg(test)]
mod tests;
