use core::{future::Future, marker::PhantomData, pin::Pin, task::Poll};

use crate::tasks::{JoinStatus, TaskId, Tasks};

/// A join handle of the spawned task, resolves once the task is finished.
pub struct JoinHandle<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> {
    tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    pub(crate) id: TaskId<TASK_SIZE>,
    _not_send: PhantomData<*const ()>,
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize>
    JoinHandle<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    pub(crate) fn new(
        tasks: &'static Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
        id: TaskId<TASK_SIZE>,
    ) -> Self {
        Self {
            tasks,
            id,
            _not_send: PhantomData,
        }
    }
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> Future
    for JoinHandle<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<()> {
        match self.tasks.set_join_waiter(self.id, cx.waker()) {
            JoinStatus::Running => Poll::Pending,
            JoinStatus::Finished => Poll::Ready(()),
        }
    }
}

impl<const TASKS: usize, const TASK_SIZE: usize, const ROOT_TASK_SIZE: usize> core::fmt::Debug
    for JoinHandle<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("JoinHandle")
    }
}

#[cfg(test)]
mod tests {
    use core::{
        cell::RefCell,
        future::{Future, pending},
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering::Relaxed},
        task::{Context, Poll, Waker},
    };

    use crate::{
        Selector,
        tasks::{JoinHandle, Tasks},
        testing::{Log, TestLoop, TestStorage, spawn_static},
    };

    type TestTasks = Tasks<2, 64, 0>;

    fn poll(handle: &mut JoinHandle<2, 64, 0>) -> Poll<()> {
        Pin::new(handle).poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn pending_until_the_task_finishes() {
        static TASKS: TestTasks = TestTasks::new();
        let mut handle = JoinHandle::new(&TASKS, spawn_static(&TASKS, async {}));
        assert_eq!(poll(&mut handle), Poll::Pending);
        TASKS.run_ready();
        assert_eq!(poll(&mut handle), Poll::Ready(()));
    }

    #[test]
    fn joining_a_finished_task_resolves_immediately() {
        static TASKS: TestTasks = TestTasks::new();
        let id = spawn_static(&TASKS, async {});
        TASKS.run_ready();
        assert_eq!(poll(&mut JoinHandle::new(&TASKS, id)), Poll::Ready(()));
    }

    #[test]
    fn dropped_handle_detaches_the_task() {
        static TASKS: TestTasks = TestTasks::new();
        static RAN: AtomicBool = AtomicBool::new(false);
        let id = spawn_static(&TASKS, async { RAN.store(true, Relaxed) });
        drop(JoinHandle::new(&TASKS, id));
        TASKS.run_ready();
        assert!(RAN.load(Relaxed));
    }

    #[test]
    fn spawned_task_can_spawn_and_join() {
        let mut selector = Selector::new().unwrap();
        let mut state = RefCell::new(Log::default());
        let event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async |ctx| {
                ctx.spawn(async |ctx| {
                    ctx.spawn(async |ctx| ctx.dispatch(1)).await;
                    ctx.dispatch(2);
                })
                .await;
                ctx.dispatch(3);
                pending::<()>().await;
            },
        );
        assert_eq!(event_loop.unwrap().state_mut().events(), [1, 2, 3]);
    }
}
