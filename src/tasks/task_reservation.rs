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
    /// # Panics
    ///
    /// Panics if:
    ///
    /// 1. the future is bigger than `TASK_SIZE` bytes
    /// 2. it is aligned to more than 16 bytes.
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

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> core::fmt::Debug
    for TaskReservation<'_, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("TaskReservation")
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
        tasks::{ReserveTask, TaskReservation, Tasks},
        testing::{claimed_forever, spawn_static},
    };

    #[test]
    fn spawning_runs_the_task() {
        static TASKS: Tasks<1, 64, 0> = Tasks::new();
        static RAN: AtomicBool = AtomicBool::new(false);
        let (id, entry) = TASKS.reserve().unwrap();
        let mut handle = TaskReservation::new(&TASKS, claimed_forever(), id, entry)
            .spawn(async { RAN.store(true, Relaxed) });
        let _ = TASKS.run_ready();
        assert!(RAN.load(Relaxed));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(Pin::new(&mut handle).poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn dropping_frees_the_slot_and_wakes_the_waiter() {
        static TASKS: Tasks<2, 64, 0> = Tasks::new();
        TASKS.queue.claim();
        let waiter = spawn_static(&TASKS, pending());
        let _ = TASKS.run_ready();
        let (id, entry) = TASKS.reserve().unwrap();
        let reservation = TaskReservation::new(&TASKS, claimed_forever(), id, entry);
        let mut reserve = ReserveTask::new(&TASKS, claimed_forever());
        let waker = TASKS.queue.waker(waiter.slot());
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut reserve).poll(&mut cx).is_pending());
        drop(reservation);
        assert!(TASKS.has_ready());
        assert!(Pin::new(&mut reserve).poll(&mut cx).is_ready());
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent tasks")]
    fn a_reserved_slot_is_not_handed_to_spawn() {
        static TASKS: Tasks<1, 64, 0> = Tasks::new();
        let (id, entry) = TASKS.reserve().unwrap();
        let _reservation = TaskReservation::new(&TASKS, claimed_forever(), id, entry);
        let _ = spawn_static(&TASKS, async {});
    }
}
