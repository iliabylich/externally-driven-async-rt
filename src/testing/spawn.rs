use core::future::Future;

use crate::tasks::{TaskEntry, TaskId, Tasks};

pub(crate) fn spawn_static<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
>(
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    future: impl Future<Output = ()> + 'static,
) -> TaskId<TASK_SIZE> {
    // SAFETY: the future is `'static`, so everything it borrows outlives every poll and drop.
    unsafe { tasks.spawn(future) }
}

pub(crate) fn spawn_root_static<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
>(
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    future: impl Future<Output = ()> + 'static,
) {
    // SAFETY: the future is `'static`, so everything it borrows outlives every poll and drop.
    unsafe { tasks.spawn_root(future) };
}

pub(crate) fn spawn_reserved_static<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
>(
    tasks: &Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    id: TaskId<TASK_SIZE>,
    entry: &'static TaskEntry<TASK_SIZE>,
    future: impl Future<Output = ()> + 'static,
) {
    // SAFETY: the future is `'static`, so everything it borrows outlives every poll and drop.
    unsafe { tasks.spawn_reserved(id, entry, future) };
}
