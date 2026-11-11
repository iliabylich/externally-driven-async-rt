use core::cell::RefCell;

use crate::{
    io::{IoIndex, IoWaiters},
    selector::{EVENTS_PER_CALL, Selector, SelectorEvent},
    storage::LoopStorage,
    tasks::TasksGuard,
    time::{Instant, Timers},
};

pub(crate) struct LoopStorageGuard<
    'a,
    S: 'static,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
    Sel = Selector,
> {
    storage: &'static LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    selector: &'a Sel,
    state: &'a RefCell<S>,
    tasks: TasksGuard<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
}

impl<
    'a,
    S: 'static,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
    Sel,
> LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, Sel>
{
    pub(crate) fn claim(
        storage: &'static LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        selector: &'a Sel,
        state: &'a RefCell<S>,
    ) -> Self {
        let tasks = TasksGuard::claim(&storage.tasks);
        Self {
            storage,
            selector,
            state,
            tasks,
        }
    }

    pub(crate) fn tasks(self) -> TasksGuard<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE> {
        self.tasks
    }

    pub(crate) fn timers(self) -> &'static Timers<TIMERS> {
        &self.storage.timers
    }

    pub(crate) fn io_waiters(self) -> &'static IoWaiters<FDS> {
        &self.storage.io_waiters
    }

    pub(crate) fn state(self) -> &'a RefCell<S> {
        self.state
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
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> LoopStorageGuard<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, Selector>
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
                        self.io_waiters()
                            .wake_ready(IoIndex::from_bits(token), ready);
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
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
    Sel,
> Clone for LoopStorageGuard<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, Sel>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<
    S: 'static,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
    Sel,
> Copy for LoopStorageGuard<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, Sel>
{
}
