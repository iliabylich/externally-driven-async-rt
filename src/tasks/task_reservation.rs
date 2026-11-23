use crate::{
    claimed::Claimed,
    tasks::{TaskEntry, TaskId, Tasks},
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
    pub(super) const fn new(
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
    ///
    /// Returns a join handle, a future that resolves once the task is finished. Dropping it
    /// detaches the task, which keeps running until it finishes.
    pub fn spawn<Fut>(
        self,
        future: Fut,
    ) -> impl Future<Output = ()> + use<TASKS, TASK_SIZE, ROOT_TASK_SIZE, Fut>
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
        tasks.join(id)
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
        pin::pin,
        sync::atomic::{AtomicBool, Ordering::Relaxed},
        task::{Context, Poll, Waker},
    };

    use crate::{
        NotEnoughResources,
        tasks::{TaskReservation, Tasks},
        testing::claimed_forever,
    };

    #[test]
    fn spawning_runs_the_task() {
        static TASKS: Tasks<1, 64, 0> = Tasks::new();
        static RAN: AtomicBool = AtomicBool::new(false);
        let (id, entry) = TASKS.pool.alloc().unwrap();
        let mut handle = pin!(
            TaskReservation::new(&TASKS, claimed_forever(), id, entry)
                .spawn(async { RAN.store(true, Relaxed) })
        );
        let _ = TASKS.run_ready();
        assert!(RAN.load(Relaxed));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(handle.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn dropping_frees_the_slot_and_wakes_the_waiter() {
        static TASKS: Tasks<2, 64, 0> = Tasks::new();
        assert!(TASKS.queue.claim().is_some());
        let waiter = TASKS.spawn_task(claimed_forever(), pending()).unwrap();
        let _ = TASKS.run_ready();
        let (id, entry) = TASKS.pool.alloc().unwrap();
        let reservation = TaskReservation::new(&TASKS, claimed_forever(), id, entry);
        let mut reserve = pin!(TASKS.reserve(claimed_forever()));
        let waker = TASKS.queue.waker(waiter.idx);
        let mut cx = Context::from_waker(&waker);
        assert!(reserve.as_mut().poll(&mut cx).is_pending());
        drop(reservation);
        assert!(!TASKS.queue.is_empty());
        assert!(reserve.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn a_reserved_slot_is_not_handed_to_spawn() {
        static TASKS: Tasks<1, 64, 0> = Tasks::new();
        let (id, entry) = TASKS.pool.alloc().unwrap();
        let _reservation = TaskReservation::new(&TASKS, claimed_forever(), id, entry);
        assert_eq!(
            TASKS.spawn_task(claimed_forever(), async {}),
            Err(NotEnoughResources::Tasks)
        );
    }
}
