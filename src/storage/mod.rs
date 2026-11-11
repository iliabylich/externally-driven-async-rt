use core::cell::RefCell;

pub(crate) use self::guard::LoopStorageGuard;
use crate::{io::IoWaiters, tasks::Tasks, time::Timers};

/// The main pre-allocated storage that [`EventLoop`](crate::EventLoop) needs.
///
/// A storage can be used by only one [`EventLoop`](crate::EventLoop) ever.
pub struct LoopStorage<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    timers: Timers<TIMERS>,
    io_waiters: IoWaiters<FDS>,
    tasks: Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
}

// SAFETY: a storage has no public methods, so other threads can only pass `&LoopStorage` around.
// The crate touches it only through a `LoopStorageGuard`, which only `LoopStorageGuard::claim`
// creates, and that succeeds once ever, on the thread that runs the claiming loop (claiming the
// tasks claims their `WakeQueue`, which panics the second time); `EventLoop` and `Context` are
// `!Send`, so everything but the wakers stays on that thread, and wakers only touch the atomic
// `WakeQueue`, which asserts that thread.
unsafe impl<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Sync for LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
}

impl<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    /// Constructor of an empty storage, **can be called in const-context**, so you can put it into a static variable.
    pub const fn new() -> Self {
        Self {
            timers: Timers::new(),
            io_waiters: IoWaiters::new(),
            tasks: Tasks::new(),
        }
    }

    pub(crate) fn claim<'a, S: 'static, Sel>(
        &'static self,
        selector: &'a Sel,
        state: &'a RefCell<S>,
    ) -> LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, Sel> {
        LoopStorageGuard::claim(self, selector, state)
    }
}

impl<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("LoopStorage")
    }
}

mod guard;

#[cfg(test)]
mod tests;
