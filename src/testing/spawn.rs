use core::future::Future;

use crate::{
    claimed::Claimed,
    tasks::{TaskEntry, TaskId, Tasks},
};

pub(crate) fn claimed_forever() -> Claimed<'static> {
    // SAFETY: `'static` never ends.
    unsafe { Claimed::new() }
}

pub(crate) fn spawn_static<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
>(
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    future: impl Future<Output = ()> + 'static,
) -> TaskId<TASK_SIZE> {
    tasks.spawn(claimed_forever(), future)
}

pub(crate) fn spawn_root_static<
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
>(
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    future: impl Future<Output = ()> + 'static,
) {
    tasks.spawn_root(claimed_forever(), future);
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
    tasks.spawn_reserved(claimed_forever(), id, entry, future);
}
