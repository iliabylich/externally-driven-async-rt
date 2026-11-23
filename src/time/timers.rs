use core::{
    cell::Cell,
    mem::DropGuard,
    task::{Poll, Waker},
};

use crate::{
    NotEnoughResources,
    data::{
        indexed_min_heap::IndexedMinHeap,
        pool::{ObjectPool, VersionedIndex},
    },
    time::Instant,
    waker_slot::WakerSlot,
};

#[must_use]
pub(crate) struct Timers<const N: usize> {
    now: fn() -> Instant,
    pool: ObjectPool<TimerEntry, N>,
    armed: IndexedMinHeap<(Instant, u64), N>,
    next_seq: Cell<u64>,
}

#[must_use]
pub(crate) struct TimerEntry {
    deadline: Cell<Instant>,
    seq: Cell<u64>,
    waker: WakerSlot,
}

pub(crate) type TimerIndex = VersionedIndex<TimerEntry>;

impl<const N: usize> Timers<N> {
    pub(crate) const fn new(now: fn() -> Instant) -> Self {
        Self {
            now,
            pool: ObjectPool::new(
                [const {
                    TimerEntry {
                        deadline: Cell::new(Instant::ZERO),
                        seq: Cell::new(0),
                        waker: WakerSlot::new(),
                    }
                }; N],
            ),
            armed: IndexedMinHeap::new(),
            next_seq: Cell::new(0),
        }
    }

    pub(crate) fn now(&self) -> Instant {
        (self.now)()
    }

    fn entry(&self, index: TimerIndex) -> &TimerEntry {
        let Some(timer) = self.pool.get_versioned(index) else {
            unreachable!("stale timer {}/{}", index.idx, index.version);
        };
        timer
    }

    pub(crate) fn alloc(
        &self,
        deadline: Instant,
        waker: &Waker,
    ) -> Result<TimerIndex, NotEnoughResources> {
        let (index, timer) = self.pool.alloc().ok_or(NotEnoughResources::Timers)?;
        let seq = self.next_seq.get();
        self.next_seq.set(seq.wrapping_add(1));
        timer.deadline.set(deadline);
        timer.seq.set(seq);
        timer.waker.store(waker);
        self.armed.insert(index.idx, (deadline, seq));
        Ok(index)
    }

    pub(crate) async fn sleep_until(&self, deadline: Instant) -> Result<(), NotEnoughResources> {
        if self.now() >= deadline {
            return Ok(());
        }
        let index =
            core::future::poll_fn(|cx| Poll::Ready(self.alloc(deadline, cx.waker()))).await?;
        let guard = DropGuard::new(index, |index| self.release(index));
        core::future::poll_fn(|cx| {
            if self.now() >= deadline {
                return Poll::Ready(());
            }
            self.set_waker(index, cx.waker());
            Poll::Pending
        })
        .await;
        drop(guard);
        Ok(())
    }

    pub(crate) fn set_waker(&self, index: TimerIndex, waker: &Waker) {
        let timer = self.entry(index);
        timer.waker.store(waker);
        if !self.armed.contains(index.idx) {
            self.armed
                .insert(index.idx, (timer.deadline.get(), timer.seq.get()));
        }
    }

    pub(crate) fn release(&self, index: TimerIndex) {
        let timer = self.entry(index);
        timer.waker.take();
        if self.armed.contains(index.idx) {
            self.armed.remove(index.idx);
        }
        self.pool.free(index);
    }

    pub(crate) fn drop_wakers(&self) {
        for (_, timer) in self.pool.iter() {
            timer.waker.take();
        }
        self.armed.clear();
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        let (_, (deadline, _)) = self.armed.peek()?;
        Some(deadline)
    }

    pub(crate) fn take_expired(&self, now: Instant) -> Option<Waker> {
        let (idx, (deadline, _)) = self.armed.peek()?;
        if deadline > now {
            return None;
        }
        self.armed.remove(idx);
        let (_, timer) = self.pool.get(idx);
        let Some(waker) = timer.waker.take() else {
            unreachable!("armed timer {idx} has no waker");
        };
        Some(waker)
    }
}

#[cfg(test)]
mod tests {
    use core::{
        cell::Cell,
        future::Future,
        pin::pin,
        sync::atomic::{AtomicU64, Ordering::Relaxed},
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use crate::{
        NotEnoughResources,
        tasks::WakeQueue,
        testing::{assert_yields, counting_waker, fake_instant},
        time::{Instant, Timers},
    };

    #[track_caller]
    fn assert_expires<const N: usize, const W: usize>(
        timers: &Timers<N>,
        wakers: &'static WakeQueue<W>,
        now: Instant,
        idxs: impl IntoIterator<Item = u32>,
    ) {
        let expired = core::iter::from_fn(|| timers.take_expired(now));
        assert_yields!(expired.map(|waker| wakers.idx_by_waker_data(&waker)), idxs);
    }

    #[test]
    fn alloc_and_release() {
        let now = Instant::now();
        let drops = Cell::new(0);
        let timers = &Timers::<2>::new(Instant::now);
        let waker = counting_waker(&drops);
        let index = timers.alloc(now, &waker).unwrap();
        assert_eq!(timers.next_deadline(), Some(now));

        timers.release(index);
        assert_eq!(drops.get(), 1);
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn capacity_is_bounded() {
        let now = Instant::now();
        let timers = &Timers::<1>::new(Instant::now);
        let _first = timers.alloc(now, Waker::noop()).unwrap();
        assert_eq!(
            timers.alloc(now, Waker::noop()).err(),
            Some(NotEnoughResources::Timers)
        );
    }

    #[test]
    fn expire_wakes_due_timers_in_deadline_order() {
        static WAKERS: WakeQueue<8> = WakeQueue::new();
        let earliest = Instant::now();
        let now = earliest.saturating_add(Duration::from_millis(1));
        let timers = &Timers::<8>::new(Instant::now);
        let _ = timers
            .alloc(
                now.saturating_add(Duration::from_millis(5)),
                &WAKERS.waker(3),
            )
            .unwrap();
        let _ = timers.alloc(now, &WAKERS.waker(2)).unwrap();
        let _ = timers
            .alloc(
                now.saturating_add(Duration::from_millis(5)),
                &WAKERS.waker(4),
            )
            .unwrap();
        let _ = timers
            .alloc(
                now.saturating_add(Duration::from_millis(10)),
                &WAKERS.waker(5),
            )
            .unwrap();
        let _ = timers.alloc(earliest, &WAKERS.waker(1)).unwrap();

        assert_expires(
            timers,
            &WAKERS,
            now.saturating_add(Duration::from_millis(5)),
            [1, 2, 3, 4],
        );
        assert_expires(
            timers,
            &WAKERS,
            now.saturating_add(Duration::from_millis(9)),
            [],
        );
        assert_eq!(
            timers.next_deadline(),
            Some(now.saturating_add(Duration::from_millis(10)))
        );
    }

    #[test]
    fn equal_deadlines_expire_in_insertion_order_across_slot_reuse() {
        static WAKERS: WakeQueue<8> = WakeQueue::new();
        let now = Instant::now();
        let timers = &Timers::<2>::new(Instant::now);
        let first = timers.alloc(now, Waker::noop()).unwrap();
        let _ = timers.alloc(now, &WAKERS.waker(1)).unwrap();
        timers.release(first);
        let _ = timers.alloc(now, &WAKERS.waker(2)).unwrap();
        assert_expires(timers, &WAKERS, now, [1, 2]);
    }

    #[test]
    fn released_timer_is_not_woken() {
        static WAKERS: WakeQueue<8> = WakeQueue::new();
        let now = Instant::now();
        let timers = &Timers::<1>::new(Instant::now);
        let index = timers.alloc(now, &WAKERS.waker(1)).unwrap();
        timers.release(index);
        assert_expires(timers, &WAKERS, now, []);
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn replaced_waker_is_the_one_woken() {
        static WAKERS: WakeQueue<8> = WakeQueue::new();
        let now = Instant::now();
        let timers = &Timers::<1>::new(Instant::now);
        let index = timers.alloc(now, &WAKERS.waker(1)).unwrap();
        timers.set_waker(index, &WAKERS.waker(2));
        assert_expires(timers, &WAKERS, now, [2]);
    }

    #[test]
    fn next_deadline_is_the_earliest_one() {
        let now = Instant::now();
        let timers = &Timers::<4>::new(Instant::now);
        assert_eq!(timers.next_deadline(), None);
        let later = now.saturating_add(Duration::from_millis(30));
        let earlier = now.saturating_add(Duration::from_millis(10));
        let _ = timers.alloc(later, Waker::noop()).unwrap();
        let earliest = timers.alloc(earlier, Waker::noop()).unwrap();
        let _ = timers
            .alloc(now.saturating_add(Duration::from_millis(20)), Waker::noop())
            .unwrap();
        assert_eq!(timers.next_deadline(), Some(earlier));
        timers.release(earliest);
        assert_eq!(
            timers.next_deadline(),
            Some(now.saturating_add(Duration::from_millis(20)))
        );
    }

    #[test]
    fn releasing_after_dropping_wakers_frees_the_slot() {
        let drops = Cell::new(0);
        let timers = &Timers::<1>::new(Instant::now);
        let waker = counting_waker(&drops);
        let index = timers.alloc(Instant::now(), &waker).unwrap();
        timers.drop_wakers();
        assert_eq!(drops.get(), 1);
        assert_eq!(timers.next_deadline(), None);
        timers.release(index);
        assert!(timers.alloc(Instant::now(), &waker).is_ok());
    }

    #[test]
    fn sleep_until_now_is_ready_without_a_timer() {
        let timers = &Timers::<1>::new(Instant::now);
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(
            pin!(timers.sleep_until(Instant::now())).poll(&mut cx),
            Poll::Ready(Ok(()))
        );
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn dropping_a_pending_sleep_releases_its_timer() {
        let timers = &Timers::<1>::new(Instant::now);
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now().saturating_add(Duration::from_secs(60));
        {
            let mut sleep = pin!(timers.sleep_until(deadline));
            assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Pending);
            assert_eq!(timers.next_deadline(), Some(deadline));
        }
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn finished_sleep_releases_its_timer() {
        static NOW: AtomicU64 = AtomicU64::new(0);
        fn now() -> Instant {
            fake_instant(&NOW)
        }
        let timers = &Timers::<1>::new(now);
        let mut cx = Context::from_waker(Waker::noop());
        let mut sleep =
            pin!(timers.sleep_until(timers.now().saturating_add(Duration::from_millis(10))));
        assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Pending);
        NOW.store(10, Relaxed);
        assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn sleep_without_a_free_timer_fails() {
        let timers = &Timers::<1>::new(Instant::now);
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now().saturating_add(Duration::from_secs(60));
        let mut first = pin!(timers.sleep_until(deadline));
        let mut second = pin!(timers.sleep_until(deadline));
        assert_eq!(first.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(
            second.as_mut().poll(&mut cx),
            Poll::Ready(Err(NotEnoughResources::Timers))
        );
    }

    #[test]
    fn repolling_a_pending_sleep_replaces_its_waker() {
        static WAKERS: WakeQueue<2> = WakeQueue::new();
        let timers = &Timers::<1>::new(Instant::now);
        let deadline = Instant::now().saturating_add(Duration::from_secs(60));
        let mut sleep = pin!(timers.sleep_until(deadline));
        for idx in [0, 1] {
            let waker = WAKERS.waker(idx);
            let mut cx = Context::from_waker(&waker);
            assert_eq!(sleep.as_mut().poll(&mut cx), Poll::Pending);
        }
        let woken = timers.take_expired(deadline).unwrap();
        assert_eq!(WAKERS.idx_by_waker_data(&woken), 1);
    }
}
