use core::{future::Future, time::Duration};

use crate::{
    Either2, NotEnoughResources,
    claimed::Claimed,
    select2,
    selector::{EVENTS_PER_CALL, Selector, SelectorEvent},
    storage::LoopStorage,
    tasks::{SlotWaiterTakenError, TaskReservation},
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
    /// Returns a join handle, a future that resolves once the task is finished. Dropping it
    /// detaches the task, which keeps running until it finishes.
    ///
    /// # Errors
    ///
    /// Returns an error if all `TASKS` task slots are taken, `f` isn't called then. Use
    /// [`reserve_task`](Self::reserve_task) to wait for a free slot instead.
    pub fn spawn<F, Fut>(
        &self,
        f: F,
    ) -> Result<
        impl Future<Output = ()> + use<E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS, F, Fut>,
        NotEnoughResources,
    >
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future<Output = ()> + 'a,
    {
        let ctx = *self;
        let id = self.storage.tasks.spawn(self.claimed, move || f(ctx))?;
        Ok(self.storage.tasks.join(id))
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
    pub fn reserve_task(
        &self,
    ) -> impl Future<
        Output = Result<
            TaskReservation<'a, TASKS, TASK_SIZE, ROOT_TASK_SIZE>,
            SlotWaiterTakenError,
        >,
    > + use<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS> {
        self.storage.tasks.reserve(self.claimed)
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
    pub fn sleep(
        &self,
        duration: Duration,
    ) -> impl Future<Output = Result<(), NotEnoughResources>>
    + use<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS> {
        let timers = &self.storage.timers;
        timers.sleep_until(timers.now().saturating_add(duration))
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
    ) -> impl Future<Output = Result<Option<F::Output>, NotEnoughResources>> {
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
    /// Notifications don't queue up: any number of `notify()` calls made before the future is
    /// polled resolve it only once.
    ///
    /// Only one task can wait for a notification at a time.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub fn notified(
        &self,
    ) -> impl Future<Output = ()> + use<'a, E, EVENTS, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>
    {
        self.storage.notification.notified()
    }

    pub(crate) fn clear(&self) {
        self.storage.tasks.drop_all();
        self.storage.timers.drop_wakers();
        self.storage.io_waiters.drop_wakers();
        self.storage.notification.clear();
        self.storage.events.clear();
    }

    pub(crate) fn process_selector_events(&self) -> rustix::io::Result<()> {
        let mut events = [None; EVENTS_PER_CALL];
        loop {
            let full = self.selector.poll_nonblocking(&mut events)?;
            for event in events.iter().map_while(|event| *event) {
                match event {
                    SelectorEvent::Io { fd, ready } => {
                        self.storage.io_waiters.wake_ready(fd, ready);
                    }
                    SelectorEvent::Notify => self.storage.notification.fire(),
                    SelectorEvent::Timer => {
                        let now = self.storage.timers.now();
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
        self.selector.flush(
            self.storage.io_waiters.pending_changes(),
            self.storage.timers.next_deadline(),
            self.storage.timers.now(),
        )?;
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

#[cfg(test)]
mod tests {
    use core::{
        future::{Future, pending},
        pin::pin,
        sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use crate::{
        Selector,
        tasks::RootStatus,
        testing::{
            EventLoopTestingExt, FlagOnDrop, TestContext, TestLoop, TestStorage, assert_yields,
            fake_instant, wake_expired_timers,
        },
        time::Instant,
    };

    #[test]
    fn future_finishing_first_wins() {
        let selector = Selector::new().unwrap();
        let (ctx, root): (TestContext<'_>, _) = {
            static STORAGE: TestStorage = TestStorage::new();
            STORAGE.claim(&selector).unwrap()
        };
        ctx.storage.tasks.spawn_root(root, ctx.claimed, async move {
            let result = ctx
                .timeout(Duration::from_secs(60), ctx.notified())
                .await
                .unwrap();
            ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
            pending::<()>().await;
        });
        assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
        assert!(ctx.storage.timers.next_deadline().is_some());
        ctx.storage.notification.fire();
        assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
        assert_yields!(ctx.storage.events.iter_removing(), [1]);
        assert_eq!(ctx.storage.timers.next_deadline(), None);
        ctx.clear();
    }

    #[test]
    fn elapsing_drops_the_future() {
        static DROPPED: AtomicBool = AtomicBool::new(false);
        static NOW: AtomicU64 = AtomicU64::new(0);
        fn now() -> Instant {
            fake_instant(&NOW)
        }
        let selector = Selector::new().unwrap();
        let (ctx, root): (TestContext<'_>, _) = {
            static STORAGE: TestStorage = TestStorage::with_clock(now);
            STORAGE.claim(&selector).unwrap()
        };
        ctx.storage.tasks.spawn_root(root, ctx.claimed, async move {
            let result = ctx
                .timeout(Duration::from_millis(10), async {
                    let _flag = FlagOnDrop::new(&DROPPED);
                    pending::<()>().await;
                })
                .await
                .unwrap();
            ctx.dispatch(result.map_or(0, |()| 1)).unwrap();
            pending::<()>().await;
        });
        assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
        assert!(!DROPPED.load(Relaxed));
        NOW.store(10, Relaxed);
        wake_expired_timers(&ctx.storage.timers);
        assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
        assert_yields!(ctx.storage.events.iter_removing(), [0]);
        assert!(DROPPED.load(Relaxed));
        assert_eq!(ctx.storage.timers.next_deadline(), None);
        ctx.clear();
    }

    #[test]
    fn ready_future_beats_a_zero_timeout() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let ctx: TestContext<'_> = STORAGE.claim(&selector).unwrap().0;
        let mut cx = Context::from_waker(Waker::noop());
        let mut timeout = pin!(ctx.timeout(Duration::ZERO, async {
            let value = 1;
            let borrowed = &value;
            core::future::ready(()).await;
            *borrowed
        }));
        assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Ready(Ok(Some(1))));
        assert_eq!(ctx.storage.timers.next_deadline(), None);
    }

    #[test]
    fn pending_pinned_future_keeps_its_timer_until_dropped() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let ctx: TestContext<'_> = STORAGE.claim(&selector).unwrap().0;
        let mut cx = Context::from_waker(Waker::noop());
        {
            let mut timeout = pin!(ctx.timeout(Duration::from_secs(60), async {
                let value = 1;
                let borrowed = &value;
                pending::<()>().await;
                *borrowed
            }));
            assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(timeout.as_mut().poll(&mut cx), Poll::Pending);
            assert!(ctx.storage.timers.next_deadline().is_some());
        }
        assert_eq!(ctx.storage.timers.next_deadline(), None);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn sleep_waits_at_least_its_duration() {
        let start = Instant::now();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            async |ctx| {
                ctx.sleep(Duration::from_millis(20)).await.unwrap();
                ctx.dispatch(1).unwrap();
            },
        )
        .unwrap();
        assert_yields!(event_loop.events(), []);
        event_loop.run_until_root_finishes();
        assert!(Instant::now().saturating_sub(start) >= Duration::from_millis(20));
        assert_yields!(event_loop.events(), [1]);
    }

    #[test]
    fn sleeps_complete_in_deadline_order() {
        static NOW: AtomicU64 = AtomicU64::new(0);
        fn now() -> Instant {
            fake_instant(&NOW)
        }
        let selector = Selector::new().unwrap();
        let (ctx, root): (TestContext<'_>, _) = {
            static STORAGE: TestStorage = TestStorage::with_clock(now);
            STORAGE.claim(&selector).unwrap()
        };
        ctx.storage.tasks.spawn_root(root, ctx.claimed, async move {
            let handles = [300, 100, 200].map(|ms| {
                ctx.spawn(async move |ctx| {
                    ctx.sleep(Duration::from_millis(ms)).await.unwrap();
                    ctx.dispatch(u32::try_from(ms).unwrap()).unwrap();
                })
                .unwrap()
            });
            for handle in handles {
                handle.await;
            }
        });
        assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Running);
        NOW.store(300, Relaxed);
        wake_expired_timers(&ctx.storage.timers);
        assert_eq!(ctx.storage.tasks.run_ready(), RootStatus::Finished);
        assert_yields!(ctx.storage.events.iter_removing(), [100, 200, 300]);
        ctx.clear();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn drift_free_interval_from_relative_sleeps() {
        let period = Duration::from_millis(10);
        let start = Instant::now();
        let mut selector = Selector::new().unwrap();
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            async move |ctx| {
                let mut next = Instant::now();
                for i in 0..5 {
                    next = next.saturating_add(period);
                    ctx.sleep(next.saturating_sub(Instant::now()))
                        .await
                        .unwrap();
                    ctx.dispatch(i).unwrap();
                }
            },
        )
        .unwrap();
        event_loop.run_until_root_finishes();
        assert_yields!(event_loop.events(), [0, 1, 2, 3, 4]);
        assert!(Instant::now().saturating_sub(start) >= period * 5);
    }
}
