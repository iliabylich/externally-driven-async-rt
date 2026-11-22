use crate::{
    claimed::Claimed,
    tasks::{JoinHandle, TaskEntry, TaskId, Tasks},
};

/// A task reservation that can be used to spawn a task without waiting for a free slot.
#[must_use = "a reservation holds a task slot until it is spawned into or dropped"]
pub struct TaskReservation<
    'a,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
> {
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    claimed: Claimed<'a>,
    id: TaskId<TASK_SIZE>,
    entry: &'static TaskEntry<TASK_SIZE>,
}

impl<'a, const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
    TaskReservation<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    pub(crate) const fn new(
        tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
        claimed: Claimed<'a>,
        id: TaskId<TASK_SIZE>,
        entry: &'static TaskEntry<TASK_SIZE>,
    ) -> Self {
        Self {
            tasks,
            claimed,
            id,
            entry,
        }
    }

    /// Spawns a task using a reserved slot that is held by this [`TaskReservation`].
    ///
    /// The future must fit `TASK_SIZE` bytes and be aligned to at most 16 bytes, otherwise the
    /// program doesn't compile.
    pub fn spawn<Fut>(self, future: Fut) -> JoinHandle<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
    where
        Fut: Future<Output = ()> + 'a,
    {
        let Self {
            tasks,
            claimed,
            id,
            entry,
        } = self;
        tasks.spawn_reserved(claimed, id, entry, future);
        core::mem::forget(self);
        JoinHandle::new(tasks, id)
    }
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> Drop
    for TaskReservation<'_, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    fn drop(&mut self) {
        self.tasks.release(self.id);
    }
}

#[cfg(test)]
mod tests {
    use core::{
        future::{Future, pending},
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering::Relaxed},
        task::{Context, Poll, Waker},
    };

    use crate::{
        tasks::{ReserveTask, TaskReservation, TasksFullError},
        testing::{TasksStorage, claimed_forever, spawn_static},
    };

    #[test]
    fn spawning_runs_the_task() {
        static STORAGE: TasksStorage<1, 64, 0> = TasksStorage::new();
        static RAN: AtomicBool = AtomicBool::new(false);
        let tasks = &STORAGE.tasks;
        let (id, entry) = tasks.pool.alloc().unwrap();
        let mut handle = TaskReservation::new(tasks, claimed_forever(), id, entry)
            .spawn(async { RAN.store(true, Relaxed) });
        let _ = tasks.run_ready();
        assert!(RAN.load(Relaxed));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(Pin::new(&mut handle).poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn dropping_frees_the_slot_and_wakes_the_waiter() {
        static STORAGE: TasksStorage<2, 64, 0> = TasksStorage::new();
        let tasks = &STORAGE.tasks;
        assert!(tasks.queue.claim().is_some());
        let waiter = spawn_static(tasks, pending()).unwrap();
        let _ = tasks.run_ready();
        let (id, entry) = tasks.pool.alloc().unwrap();
        let reservation = TaskReservation::new(tasks, claimed_forever(), id, entry);
        let mut reserve = ReserveTask::new(tasks, claimed_forever());
        let waker = tasks.queue.waker(waiter.idx);
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut reserve).poll(&mut cx).is_pending());
        drop(reservation);
        assert!(!tasks.queue.is_empty());
        assert!(Pin::new(&mut reserve).poll(&mut cx).is_ready());
    }

    #[test]
    fn a_reserved_slot_is_not_handed_to_spawn() {
        static STORAGE: TasksStorage<1, 64, 0> = TasksStorage::new();
        let tasks = &STORAGE.tasks;
        let (id, entry) = tasks.pool.alloc().unwrap();
        let _reservation = TaskReservation::new(tasks, claimed_forever(), id, entry);
        assert_eq!(spawn_static(tasks, async {}), Err(TasksFullError));
    }
}
