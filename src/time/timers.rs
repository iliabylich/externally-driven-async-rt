use core::{cell::Cell, task::Waker};

use crate::{
    pool::ObjectPool,
    time::{Instant, TimerEntry, TimerIndex},
};

pub(crate) struct Timers<const N: usize> {
    pool: ObjectPool<TimerEntry, N>,
    next_seq: Cell<u64>,
}

impl<const N: usize> Timers<N> {
    pub(crate) const fn new() -> Self {
        Self {
            pool: ObjectPool::new([const { TimerEntry::new() }; N]),
            next_seq: Cell::new(0),
        }
    }

    pub(crate) fn alloc(&self, deadline: Instant, waker: Waker) -> (TimerIndex, &TimerEntry) {
        let Some((index, timer)) = self.pool.alloc() else {
            panic!("more than {N} concurrent timers");
        };
        let seq = self.next_seq.get();
        self.next_seq.set(seq + 1);
        timer.arm(deadline, seq, waker);
        (index, timer)
    }

    pub(crate) fn release(&self, index: TimerIndex) {
        if let Some(timer) = self.pool.get(index) {
            timer.take_waker();
            self.pool.free(index);
        }
    }

    pub(crate) fn clear(&self) {
        for (index, _) in self.pool.iter() {
            self.release(index);
        }
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.pool
            .iter()
            .filter(|(_, timer)| timer.has_waker())
            .map(|(_, timer)| timer.deadline())
            .min()
    }

    pub(crate) fn take_expired(&self, now: Instant) -> Option<Waker> {
        let (_, timer) = self
            .pool
            .iter()
            .filter(|(_, timer)| timer.has_waker() && timer.deadline() <= now)
            .min_by_key(|(_, timer)| timer.expiry())?;
        timer.take_waker()
    }
}

#[cfg(test)]
mod tests {
    use core::{cell::Cell, task::Waker, time::Duration};

    use crate::{
        tasks::WakeQueue,
        testing::{assert_yields, counting_waker},
        time::{Instant, Timers},
    };

    #[track_caller]
    fn assert_expires<const N: usize, const W: usize>(
        timers: &Timers<N>,
        wakers: &'static WakeQueue<W>,
        now: Instant,
        ids: impl IntoIterator<Item = u32>,
    ) {
        let expired = core::iter::from_fn(|| timers.take_expired(now));
        assert_yields!(expired.map(|waker| wakers.id_by_waker_data(&waker)), ids);
    }

    #[test]
    fn alloc_and_release() {
        let now = Instant::now();
        let drops = Cell::new(0);
        let timers = &Timers::<2>::new();
        let (index, _) = timers.alloc(now, counting_waker(&drops));
        assert_eq!(timers.next_deadline(), Some(now));

        timers.release(index);
        assert_eq!(drops.get(), 1);
        assert_eq!(timers.next_deadline(), None);
        timers.release(index);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent timers")]
    fn capacity_is_bounded() {
        let now = Instant::now();
        let timers = &Timers::<1>::new();
        timers.alloc(now, Waker::noop().clone());
        timers.alloc(now, Waker::noop().clone());
    }

    #[test]
    fn expire_wakes_due_timers_in_deadline_order() {
        static WAKERS: WakeQueue<8> = WakeQueue::new();
        let earliest = Instant::now();
        let now = earliest + Duration::from_millis(1);
        let timers = &Timers::<8>::new();
        timers.alloc(now + Duration::from_millis(5), WAKERS.waker(3));
        timers.alloc(now, WAKERS.waker(2));
        timers.alloc(now + Duration::from_millis(5), WAKERS.waker(4));
        timers.alloc(now + Duration::from_millis(10), WAKERS.waker(5));
        timers.alloc(earliest, WAKERS.waker(1));

        assert_expires(
            timers,
            &WAKERS,
            now + Duration::from_millis(5),
            [1, 2, 3, 4],
        );
        assert_expires(timers, &WAKERS, now + Duration::from_millis(9), []);
        assert_eq!(
            timers.next_deadline(),
            Some(now + Duration::from_millis(10))
        );
    }

    #[test]
    fn equal_deadlines_expire_in_insertion_order_across_slot_reuse() {
        static WAKERS: WakeQueue<8> = WakeQueue::new();
        let now = Instant::now();
        let timers = &Timers::<2>::new();
        let (first, _) = timers.alloc(now, Waker::noop().clone());
        timers.alloc(now, WAKERS.waker(1));
        timers.release(first);
        timers.alloc(now, WAKERS.waker(2));
        assert_expires(timers, &WAKERS, now, [1, 2]);
    }

    #[test]
    fn released_timer_is_not_woken() {
        static WAKERS: WakeQueue<8> = WakeQueue::new();
        let now = Instant::now();
        let timers = &Timers::<1>::new();
        let (index, _) = timers.alloc(now, WAKERS.waker(1));
        timers.release(index);
        assert_expires(timers, &WAKERS, now, []);
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn replaced_waker_is_the_one_woken() {
        static WAKERS: WakeQueue<8> = WakeQueue::new();
        let now = Instant::now();
        let timers = &Timers::<1>::new();
        let (_, timer) = timers.alloc(now, WAKERS.waker(1));
        timer.set_waker(&WAKERS.waker(2));
        assert_expires(timers, &WAKERS, now, [2]);
    }

    #[test]
    fn next_deadline_is_the_earliest_one() {
        let now = Instant::now();
        let timers = &Timers::<4>::new();
        assert_eq!(timers.next_deadline(), None);
        let later = now + Duration::from_millis(30);
        let earlier = now + Duration::from_millis(10);
        timers.alloc(later, Waker::noop().clone());
        let (earliest, _) = timers.alloc(earlier, Waker::noop().clone());
        timers.alloc(now + Duration::from_millis(20), Waker::noop().clone());
        assert_eq!(timers.next_deadline(), Some(earlier));
        timers.release(earliest);
        assert_eq!(
            timers.next_deadline(),
            Some(now + Duration::from_millis(20))
        );
    }
}
