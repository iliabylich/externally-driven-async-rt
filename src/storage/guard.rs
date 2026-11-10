use core::cell::RefCell;

use crate::{
    io::IoWaiters,
    selector::{EVENTS_PER_CALL, Selector, SelectorEvent},
    storage::LoopStorage,
    tasks::TasksGuard,
    time::{Instant, Timers},
};

pub(crate) struct LoopStorageGuard<
    'a,
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    Sel = Selector,
> {
    storage: &'static LoopStorage<
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
    >,
    selector: &'a Sel,
    tasks: TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
}

impl<
    'a,
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    Sel,
>
    LoopStorageGuard<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Sel,
    >
{
    pub(crate) fn claim(
        storage: &'static LoopStorage<
            S,
            MAX_TASKS_COUNT,
            MAX_TASK_SIZE,
            MAX_ROOT_TASK_SIZE,
            MAX_TIMERS_COUNT,
            MAX_IO_COUNT,
        >,
        selector: &'a Sel,
        state: S,
    ) -> Self {
        let tasks = TasksGuard::claim(&storage.tasks);
        *storage.state.borrow_mut() = Some(state);
        Self {
            storage,
            selector,
            tasks,
        }
    }

    pub(crate) fn tasks(
        self,
    ) -> TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE> {
        self.tasks
    }

    pub(crate) fn timers(self) -> &'static Timers<MAX_TIMERS_COUNT> {
        &self.storage.timers
    }

    pub(crate) fn io_waiters(self) -> &'static IoWaiters<MAX_IO_COUNT> {
        &self.storage.io_waiters
    }

    pub(crate) fn state_slot(self) -> &'static RefCell<Option<S>> {
        &self.storage.state
    }

    pub(crate) fn clear(self) {
        self.tasks.drop_all();
        self.timers().clear();
        self.io_waiters().clear();
    }
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
    LoopStorageGuard<
        'a,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Selector,
    >
{
    pub(crate) fn selector(self) -> &'a Selector {
        self.selector
    }

    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector().poll_nonblocking(&mut events)?;
            for event in events.iter().map_while(|event| *event) {
                match event {
                    SelectorEvent::Io { token, ready } => {
                        self.io_waiters().wake_ready(token, ready);
                    }
                    SelectorEvent::Timer => {
                        let now = Instant::now();
                        while let Some(waker) = self.timers().take_expired(now) {
                            waker.wake();
                        }
                    }
                }
            }
            if !full {
                return Ok(());
            }
        }
    }

    pub(crate) fn flush(&self) -> rustix::io::Result<()> {
        let selector = self.selector();
        match self.timers().next_deadline() {
            Some(deadline) => selector.arm_timer(deadline),
            None => selector.disarm_timer(),
        }
        selector.flush(self.io_waiters().pending_changes())?;
        self.io_waiters().mark_flushed();
        Ok(())
    }
}

impl<
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    Sel,
> Clone
    for LoopStorageGuard<
        '_,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Sel,
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
    Sel,
> Copy
    for LoopStorageGuard<
        '_,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Sel,
    >
{
}
