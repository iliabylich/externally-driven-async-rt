use core::cell::UnsafeCell;

use crate::{
    instant::Instant,
    selectors::{Direction, EVENTS_PER_CALL, Selector, SelectorEvent},
    storage::{IoWaiters, TasksGuard, Timers},
};

pub(crate) struct StorageGuard<
    'a,
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    Sel: 'static = Selector,
> {
    pub(crate) selector: &'static UnsafeCell<Option<Sel>>,
    pub(crate) state: &'static UnsafeCell<Option<S>>,
    pub(crate) timers: &'static Timers<MAX_TIMERS_COUNT>,
    pub(crate) io_waiters: &'static IoWaiters<MAX_IO_COUNT>,
    pub(crate) tasks: TasksGuard<'a, MAX_TASKS_COUNT, MAX_TASK_SIZE, MAX_ROOT_TASK_SIZE>,
}

impl<
    'a,
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
    Sel: 'static,
>
    StorageGuard<
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
    pub(crate) fn clear(self) {
        self.tasks.drop_all();
        self.timers.clear();
        self.io_waiters.clear();
    }
}

impl<
    S: 'static,
    const MAX_TASKS_COUNT: usize,
    const MAX_TASK_SIZE: usize,
    const MAX_ROOT_TASK_SIZE: usize,
    const MAX_TIMERS_COUNT: usize,
    const MAX_IO_COUNT: usize,
>
    StorageGuard<
        '_,
        S,
        MAX_TASKS_COUNT,
        MAX_TASK_SIZE,
        MAX_ROOT_TASK_SIZE,
        MAX_TIMERS_COUNT,
        MAX_IO_COUNT,
        Selector,
    >
{
    pub(crate) fn selector(&self) -> &Selector {
        // SAFETY: the selector is written only in `claim`, before any reference to it can exist,
        // and taken only in `EventLoop`'s `drop`, after the tasks (which hold the only other
        // references, besides the loop's own borrows) are gone.
        unsafe { &*self.selector.get() }
            .as_ref()
            .expect("the selector is set until the loop is dropped")
    }

    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector().poll_nonblocking(&mut events)?;
            for event in events.iter().map_while(|event| *event) {
                match event {
                    SelectorEvent::Readable(token) => {
                        if let Some(waker) = self.io_waiters.take_waker(token, Direction::Read) {
                            waker.wake();
                        }
                    }
                    SelectorEvent::Writable(token) => {
                        if let Some(waker) = self.io_waiters.take_waker(token, Direction::Write) {
                            waker.wake();
                        }
                    }
                    SelectorEvent::Timer => {
                        let now = Instant::now();
                        while let Some(waker) = self.timers.take_expired(now) {
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
        match self.timers.next_deadline() {
            Some(deadline) => selector.arm_timer(deadline),
            None => selector.disarm_timer(),
        }
        selector.flush(self.io_waiters.pending_changes())?;
        self.io_waiters.mark_flushed();
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
    Sel: 'static,
> Clone
    for StorageGuard<
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
    Sel: 'static,
> Copy
    for StorageGuard<
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
