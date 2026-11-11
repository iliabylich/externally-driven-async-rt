use core::{future::Future, time::Duration};

use crate::{
    state::State,
    storage::LoopStorageGuard,
    tasks::{JoinHandle, ReserveTask},
    time::{Instant, Sleep, Timeout},
};

/// A handle of the event loop that you are currently in.
///
/// Used to spawn tasks, sleep, dispatch events to the state and start IO operations.
pub struct Context<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    storage: LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    pub(crate) fn new(
        storage: LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    ) -> Self {
        Self { storage }
    }

    pub(crate) fn storage(
        &self,
    ) -> &LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS> {
        &self.storage
    }

    /// Dispatches given `event` on a state that was given to [`EventLoop`](crate::EventLoop).
    pub fn dispatch(&self, event: S::Event) {
        self.storage.state().borrow_mut().apply(event);
    }

    /// Spawns a task with a given async closure.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// 1. all `TASKS` task slots are taken
    /// 2. if the future is bigger than `TASK_SIZE` bytes
    /// 3. if it is aligned to more than 16 bytes
    ///
    /// Use [`reserve_task`](Self::reserve_task) to wait for a free slot instead.
    pub fn spawn<F, Fut>(&self, f: F) -> JoinHandle<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let id = self.storage.tasks().spawn(f(*self));
        JoinHandle::new(self.storage.tasks(), id)
    }

    /// Reserves a task slot.
    ///
    /// This method returns a future that is resolved once a slot in the internal
    /// pool of tasks is available.
    ///
    /// The future stays pending until some other task finishes and frees a slot.
    pub fn reserve_task(
        &self,
    ) -> ReserveTask<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS> {
        ReserveTask::new(self.storage)
    }

    pub(crate) fn spawn_root<F, Fut>(
        storage: LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        f: F,
    ) where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        storage.tasks().spawn_root(f(Self::new(storage)));
    }

    /// Sleeps for `duration`.
    pub fn sleep(&self, duration: Duration) -> Sleep<'_, TIMERS> {
        Sleep::new(self.storage.timers(), Instant::now() + duration)
    }

    /// Runs `future` with a timeout of `duration`, resolves to `None` in case of a timeout.
    pub fn timeout<F: Future>(&self, duration: Duration, future: F) -> Timeout<'_, F, TIMERS> {
        Timeout::new(future, self.sleep(duration))
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Clone for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Copy for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Context")
    }
}
