use core::{future::Future, pin::Pin, task::Poll};

use crate::{
    claimed::Claimed,
    tasks::{TaskReservation, Tasks},
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
    type Output = TaskReservation<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>;

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let tasks = this.tasks;
        if let Some((id, entry)) = tasks.reserve() {
            if this.waiting {
                tasks.clear_slot_waiter();
                this.waiting = false;
            }
            Poll::Ready(TaskReservation::new(tasks, this.claimed, id, entry))
        } else {
            assert!(
                this.waiting || !tasks.has_slot_waiter(),
                "only one task can wait for a free task slot"
            );
            tasks.set_slot_waiter(cx.waker());
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
            self.tasks.clear_slot_waiter();
        }
    }
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> core::fmt::Debug
    for ReserveTask<'_, TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("ReserveTask")
    }
}

#[cfg(test)]
mod tests {
    use core::{
        future::{Future, pending},
        pin::Pin,
        task::{Context, Waker},
    };

    use crate::{
        tasks::{ReserveTask, Tasks},
        testing::{claimed_forever, spawn_static},
    };

    type TestTasks = Tasks<1, 64, 0>;

    fn poll(reserve: &mut ReserveTask<'static, 1, 64, 0>) -> bool {
        Pin::new(reserve)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    #[test]
    fn waits_until_a_task_finishes() {
        static TASKS: TestTasks = TestTasks::new();
        let _ = spawn_static(&TASKS, async {});
        let mut reserve = ReserveTask::new(&TASKS, claimed_forever());
        assert!(!poll(&mut reserve));
        assert!(TASKS.has_slot_waiter());
        let _ = TASKS.run_ready();
        assert!(poll(&mut reserve));
        assert!(!TASKS.has_slot_waiter());
    }

    #[test]
    #[should_panic(expected = "only one task can wait for a free task slot")]
    fn a_second_waiter_panics() {
        static TASKS: TestTasks = TestTasks::new();
        let _ = spawn_static(&TASKS, pending());
        let mut first = ReserveTask::new(&TASKS, claimed_forever());
        let mut second = ReserveTask::new(&TASKS, claimed_forever());
        assert!(!poll(&mut first));
        poll(&mut second);
    }
}
