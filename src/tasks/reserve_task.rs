use core::{future::Future, pin::Pin, task::Poll};

use crate::{
    claimed::Claimed,
    tasks::{SlotWaiterTakenError, TaskReservation, Tasks},
};

/// A future to obtain a [`TaskReservation`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReserveTask<'a, const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
{
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    claimed: Claimed<'a>,
    waiting: bool,
}

impl<'a, const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
    ReserveTask<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    pub(crate) const fn new(
        tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
        claimed: Claimed<'a>,
    ) -> Self {
        Self {
            tasks,
            claimed,
            waiting: false,
        }
    }
}

impl<'a, const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> Future
    for ReserveTask<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    type Output =
        Result<TaskReservation<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>, SlotWaiterTakenError>;

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let tasks = this.tasks;
        if let Some((id, entry)) = tasks.pool.alloc() {
            if this.waiting {
                tasks.slot_waiter.take();
                this.waiting = false;
            }
            Poll::Ready(Ok(TaskReservation::new(tasks, this.claimed, id, entry)))
        } else if !this.waiting && tasks.slot_waiter.is_set() {
            Poll::Ready(Err(SlotWaiterTakenError))
        } else {
            tasks.slot_waiter.store(cx.waker());
            this.waiting = true;
            Poll::Pending
        }
    }
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> Drop
    for ReserveTask<'_, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    fn drop(&mut self) {
        if self.waiting {
            self.tasks.slot_waiter.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use core::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    use crate::{
        tasks::{ReserveTask, SlotWaiterTakenError},
        testing::{TasksStorage, claimed_forever, spawn_static},
    };

    type Storage = TasksStorage<1, 64, 0>;

    fn poll(reserve: &mut ReserveTask<'static, 1, 64, 0>) -> bool {
        Pin::new(reserve)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    fn poll_err(reserve: &mut ReserveTask<'static, 1, 64, 0>) -> Option<SlotWaiterTakenError> {
        match Pin::new(reserve).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => result.err(),
            Poll::Pending => None,
        }
    }

    #[test]
    fn waits_until_a_task_finishes() {
        static STORAGE: Storage = Storage::new();
        let tasks = &STORAGE.tasks;
        let _ = spawn_static(tasks, async {}).unwrap();
        let mut reserve = ReserveTask::new(tasks, claimed_forever());
        assert!(!poll(&mut reserve));
        assert!(tasks.slot_waiter.is_set());
        let _ = tasks.run_ready();
        assert!(poll(&mut reserve));
        assert!(!tasks.slot_waiter.is_set());
    }

    #[test]
    fn a_second_waiter_gets_an_error() {
        static STORAGE: Storage = Storage::new();
        let tasks = &STORAGE.tasks;
        let _ = spawn_static(tasks, async {}).unwrap();
        let mut first = ReserveTask::new(tasks, claimed_forever());
        let mut second = ReserveTask::new(tasks, claimed_forever());
        assert!(!poll(&mut first));
        assert_eq!(poll_err(&mut second), Some(SlotWaiterTakenError));
        assert!(tasks.slot_waiter.is_set());
        let _ = tasks.run_ready();
        assert!(poll(&mut first));
    }
}
