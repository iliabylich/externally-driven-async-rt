use core::{
    cell::{Cell, RefCell},
    future::Future,
    marker::PhantomData,
    time::Duration,
};

use crate::{
    io::IoIndex,
    selector::{EVENTS_PER_CALL, Selector, SelectorEvent},
    state::State,
    storage::LoopStorage,
    tasks::{JoinHandle, ReserveTask},
    time::{Instant, Sleep, Timeout},
};

/// A handle of the event loop that you are currently in.
///
/// Used to spawn tasks, sleep, dispatch events to the state and start IO operations.
pub struct Context<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    pub(crate) storage: &'static LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    pub(crate) selector: &'a Selector,
    pub(crate) state: &'a RefCell<S>,
    _invariant_not_send: PhantomData<(Cell<&'a ()>, *const ())>,
}

impl<
    'a,
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Context<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    pub(crate) fn claim(
        storage: &'static LoopStorage<TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        selector: &'a Selector,
        state: &'a RefCell<S>,
    ) -> Self {
        storage.tasks.queue.claim();
        Self {
            storage,
            selector,
            state,
            _invariant_not_send: PhantomData,
        }
    }

    /// Dispatches given `event` on a state that was given to [`EventLoop`](crate::EventLoop).
    pub fn dispatch(&self, event: S::Event) {
        self.state.borrow_mut().apply(event);
    }

    /// Spawns a task with a given async closure.
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// 1. all `TASKS` task slots are taken
    /// 2. if the future is bigger than `TASK_SIZE` bytes
    /// 3. if it is aligned to more than 16 bytes
    ///
    /// Use [`reserve_task`](Self::reserve_task) to wait for a free slot instead.
    pub fn spawn<F, Fut>(&self, f: F) -> JoinHandle<TASKS, TASK_SIZE, ROOT_TASK_SIZE>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let future = f(*self);
        // SAFETY: futures are written into `Tasks` only through a `Context`, and `Context::claim`
        // creates a context once per storage (it claims the `WakeQueue`, which panics the second
        // time; copies share the context's invariant `'a`), so every future in it outlives this
        // same `'a`. They are polled and dropped only while that loop runs: `EventLoop` drops
        // them all before `'a` ends, and the storage can never be claimed again.
        let id = unsafe { self.storage.tasks.spawn(future) };
        JoinHandle::new(&self.storage.tasks, id)
    }

    /// Reserves a task slot.
    ///
    /// This method returns a future that is resolved once a slot in the internal
    /// pool of tasks is available.
    ///
    /// The future stays pending until some other task finishes and frees a slot.
    pub fn reserve_task(
        &self,
    ) -> ReserveTask<'a, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS> {
        ReserveTask::new(*self)
    }

    /// Sleeps for `duration`.
    pub fn sleep(&self, duration: Duration) -> Sleep<'_, TIMERS> {
        Sleep::new(
            &self.storage.timers,
            Instant::now().saturating_add(duration),
        )
    }

    /// Runs `future` with a timeout of `duration`, resolves to `None` in case of a timeout.
    pub fn timeout<F: Future>(&self, duration: Duration, future: F) -> Timeout<'_, F, TIMERS> {
        Timeout::new(future, self.sleep(duration))
    }

    pub(crate) fn clear(&self) {
        self.storage.tasks.drop_all();
        self.storage.timers.clear();
        self.storage.io_waiters.clear();
    }

    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector.poll_nonblocking(&mut events)?;
            for event in events.iter().map_while(|event| *event) {
                match event {
                    SelectorEvent::Io { token, ready } => {
                        self.storage
                            .io_waiters
                            .wake_ready(IoIndex::from_bits(token), ready);
                    }
                    SelectorEvent::Timer => {
                        let now = Instant::now();
                        while let Some(waker) = self.storage.timers.take_expired(now) {
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
        match self.storage.timers.next_deadline() {
            Some(deadline) => self.selector.arm_timer(deadline),
            None => self.selector.disarm_timer(),
        }
        self.selector
            .flush(self.storage.io_waiters.pending_changes())?;
        self.storage.io_waiters.mark_flushed();
        Ok(())
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Clone for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Copy for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
}

impl<
    S: State,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Context")
    }
}
