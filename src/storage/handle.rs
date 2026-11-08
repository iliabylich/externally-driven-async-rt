use core::{cell::Cell, future::Future, marker::PhantomData, ops::Deref};

#[cfg(test)]
use core::task::Waker;

use crate::storage::{LoopStorage, RootStatus, TaskId};

pub(crate) struct Storage<
    'a,
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> {
    storage: &'static LoopStorage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    futures: PhantomData<(Cell<&'a ()>, *const ())>,
}

impl<
    'a,
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>
    Storage<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    pub(super) fn new(
        storage: &'static LoopStorage<
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
    ) -> Self {
        Self {
            storage,
            futures: PhantomData,
        }
    }

    pub(crate) fn spawn<Fut>(self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'a,
    {
        // SAFETY: a handle is only created by `LoopStorage::claim`, which succeeds once per
        // storage, so every handle to this storage has the same `'a`, which is invariant. Every
        // future therefore outlives `'a`, and the storage's futures are only polled and dropped
        // through a handle, i.e. before `'a` ends.
        unsafe { self.storage.tasks.spawn(future) }
    }

    #[cfg(test)]
    pub(crate) fn insert<Fut>(self, future: Fut) -> TaskId
    where
        Fut: Future<Output = ()> + 'a,
    {
        // SAFETY: as in `spawn`.
        unsafe { self.storage.tasks.insert(future) }
    }

    pub(crate) fn spawn_root<Fut>(self, future: Fut)
    where
        Fut: Future<Output = ()> + 'a,
    {
        // SAFETY: as in `spawn`.
        unsafe { self.storage.tasks.spawn_root(future) }
    }

    pub(crate) fn run_ready(self) -> RootStatus {
        // SAFETY: every future outlives `'a` (see `spawn`), which this handle's existence proves
        // hasn't ended.
        unsafe { self.storage.tasks.run_ready() }
    }

    #[cfg(test)]
    pub(crate) fn remove(self, id: TaskId) -> Option<Option<Waker>> {
        // SAFETY: as in `run_ready`.
        unsafe { self.storage.tasks.remove(id) }
    }

    pub(crate) fn drop_all(self) {
        // SAFETY: as in `run_ready`.
        unsafe { self.storage.tasks.drop_all() }
    }

    #[cfg(test)]
    pub(crate) fn waker(self, id: TaskId) -> Waker {
        self.storage.tasks.queue().waker(id.slot())
    }

    pub(crate) fn clear(self) {
        self.drop_all();
        self.storage.timers.clear();
        self.storage.io_waiters.clear();
    }
}

impl<
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Clone
    for Storage<
        '_,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
> Copy
    for Storage<
        '_,
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
> Deref
    for Storage<
        '_,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >
{
    type Target = LoopStorage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >;

    fn deref(&self) -> &Self::Target {
        self.storage
    }
}
