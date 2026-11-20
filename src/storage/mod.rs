use crate::{
    claimed::Claimed, context::Context, event_queue::EventQueue, io::IoWaiters,
    notify::Notification, selector::Selector, tasks::Tasks, time::Timers,
};

/// The main pre-allocated storage that [`EventLoop`](crate::EventLoop) needs.
///
/// A storage can be used by only one [`EventLoop`](crate::EventLoop) ever.
#[must_use]
pub struct LoopStorage<
    E,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> {
    pub(crate) timers: Timers<TIMERS>,
    pub(crate) io_waiters: IoWaiters<FDS>,
    pub(crate) tasks: Tasks<TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
    pub(crate) notification: Notification,
    pub(crate) events: EventQueue<E, EVENTS>,
}

// SAFETY: a storage has no public methods, so other threads can only pass `&LoopStorage` around.
// The crate touches it only through a `Context`, which only `LoopStorage::claim`
// creates, and that succeeds once ever, on the thread that runs the claiming loop (claiming the
// tasks claims their `WakeQueue`, which panics the second time); `EventLoop` and `Context` are
// `!Send`, so everything but the wakers stays on that thread, and wakers only touch the atomic
// `WakeQueue`, which asserts that thread.
unsafe impl<
    E,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> Sync for LoopStorage<E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
}

impl<
    E,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> LoopStorage<E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    /// Constructor of an empty storage, **can be called in const-context**, so you can put it into a static variable.
    pub const fn new() -> Self {
        Self {
            timers: Timers::new(),
            io_waiters: IoWaiters::new(),
            tasks: Tasks::new(),
            notification: Notification::new(),
            events: EventQueue::new(),
        }
    }

    pub(crate) fn claim<'a>(
        &'static self,
        selector: &'a Selector,
    ) -> Option<Context<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>> {
        if !self.tasks.queue.claim() {
            return None;
        }
        Some(Context {
            storage: self,
            selector,
            // SAFETY: claiming the queue above fails unless this is the storage's first claim, so
            // these tasks belong to this loop alone. Only the `EventLoop<'a>` that owns this claim
            // polls and drops them, and it drops them all before `'a` ends or, if leaked, never
            // touches them again.
            claimed: unsafe { Claimed::new() },
        })
    }
}

impl<
    E,
    const EVENTS: usize,
    const TASKS: usize,
    const TASK_SIZE: usize,
    const ROOT_TASK_SIZE: usize,
    const TIMERS: usize,
    const FDS: usize,
> core::fmt::Debug for LoopStorage<E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("LoopStorage")
    }
}

#[cfg(test)]
mod tests;
