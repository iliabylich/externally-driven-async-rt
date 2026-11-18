use core::{future::Future, time::Duration};

use crate::{
    Either2,
    claimed::Claimed,
    io::IoIndex,
    notify::Notified,
    select2,
    selector::{EVENTS_PER_CALL, Selector, SelectorEvent},
    storage::LoopStorage,
    tasks::{JoinHandle, ReserveTask, TasksFullError},
    time::{Instant, Sleep, TimersFullError},
};

/// A handle of the event loop that you are currently in.
///
/// Used to spawn tasks, sleep, dispatch events to the external world and start IO operations.
///
/// # Panics
///
/// Wakers can only be woken on the same thread that created an
/// [`EventLoop`](crate::EventLoop), otherwise waking panics.
#[must_use]
pub struct Context<
    'a,
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    pub(crate) storage:
        &'static LoopStorage<E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    pub(crate) selector: &'a Selector,
    pub(crate) claimed: Claimed<'a>,
}

impl<
    'a,
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Context<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    pub(crate) fn claim(
        storage: &'static LoopStorage<E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        selector: &'a Selector,
    ) -> Option<Self> {
        if !storage.tasks.queue.claim() {
            return None;
        }
        Some(Self {
            storage,
            selector,
            // SAFETY: claiming the queue above fails unless this is the storage's first claim, so
            // these tasks belong to this loop alone. Only the `EventLoop<'a>` that owns this claim
            // polls and drops them, and it drops them all before `'a` ends or, if leaked, never
            // touches them again.
            claimed: unsafe { Claimed::new() },
        })
    }

    /// Pushes given `event` into the queue that [`EventLoop::events`](crate::EventLoop::events)
    /// pops from.
    ///
    /// # Errors
    ///
    /// Returns `event` back if the queue already holds `EVENTS` events.
    pub fn dispatch(&self, event: E) -> Result<(), E> {
        self.storage.events.push(event)
    }

    /// Spawns a task with a given async closure.
    ///
    /// The future must fit `TASK_SIZE` bytes and be aligned to at most 16 bytes, otherwise the
    /// program doesn't compile.
    ///
    /// # Errors
    ///
    /// Returns an error if all `TASKS` task slots are taken, `f` isn't called then. Use
    /// [`reserve_task`](Self::reserve_task) to wait for a free slot instead.
    pub fn spawn<F, Fut>(
        &self,
        f: F,
    ) -> Result<JoinHandle<TASKS, TASK_SIZE, ROOT_TASK_SIZE>, TasksFullError>
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let ctx = *self;
        let id = self.storage.tasks.spawn(self.claimed, move || f(ctx))?;
        Ok(JoinHandle::new(&self.storage.tasks, id))
    }

    /// Reserves a task slot.
    ///
    /// This method returns a future that is resolved once a slot in the internal
    /// pool of tasks is available.
    ///
    /// The future stays pending until some other task finishes and frees a slot.
    ///
    /// # Errors
    ///
    /// Only one task can wait for a free slot at a time. The future resolves to
    /// [`SlotWaiterTakenError`](crate::SlotWaiterTakenError) if all `TASKS` slots are taken and another task already waits.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub const fn reserve_task(&self) -> ReserveTask<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE> {
        ReserveTask::new(&self.storage.tasks, self.claimed)
    }

    /// Sleeps for `duration`.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an error if all `TIMERS` timer slots are taken.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub fn sleep(&self, duration: Duration) -> Sleep<'_, TIMERS> {
        Sleep::new(
            &self.storage.timers,
            Instant::now().saturating_add(duration),
        )
    }

    /// Runs `future` with a timeout of `duration`, resolves to `Ok(None)` in case of a timeout.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an error if all `TIMERS` timer slots are taken.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe if `future` is.
    pub fn timeout<F: Future>(
        &self,
        duration: Duration,
        future: F,
    ) -> impl Future<Output = Result<Option<F::Output>, TimersFullError>> {
        let sleep = self.sleep(duration);
        async move {
            match select2(future, sleep).await {
                Either2::Left(output) => Ok(Some(output)),
                Either2::Right(Ok(())) => Ok(None),
                Either2::Right(Err(err)) => Err(err),
            }
        }
    }

    /// Waits until [`EventLoop::notify`](crate::EventLoop::notify) is called.
    ///
    /// See [`Notified`] for details.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub const fn notified(&self) -> Notified<'_> {
        Notified::new(&self.storage.notification)
    }

    pub(crate) fn clear(&self) {
        self.storage.tasks.drop_all();
        self.storage.timers.clear();
        self.storage.io_waiters.clear();
        self.storage.notification.clear();
        self.storage.events.clear();
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
                            .wake_ready(IoIndex::unpack(token), ready);
                    }
                    SelectorEvent::Notify => self.storage.notification.fire(),
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
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Clone for Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Copy for Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
}

impl<
    E: 'static,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for Context<'_, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Context")
    }
}
