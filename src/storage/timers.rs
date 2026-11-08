use core::{cell::Cell, mem::MaybeUninit, task::Waker};

use crate::{
    instant::Instant,
    pool::{Index, ObjectPool},
    storage::store_waker,
};

#[derive(Default)]
pub(crate) struct Timer {
    deadline: Cell<Instant>,
    seq: Cell<u64>,
    waker: Cell<Option<Waker>>,
}

pub(crate) type TimerIndex = Index<Timer>;

pub(crate) struct Timers<const N: usize> {
    pool: ObjectPool<Timer, N>,
    next_seq: Cell<u64>,
}

impl<const N: usize> Timers<N> {
    pub(crate) fn init(place: &mut MaybeUninit<Self>) -> &Self {
        let ptr = place.as_mut_ptr();
        // SAFETY: `ptr` points to `place`, borrowed exclusively until the end; the fields are
        // disjoint, `&raw mut` creates no reference to them while they're uninitialized, and
        // `MaybeUninit<T>` has `T`'s layout.
        let (pool, next_seq) = unsafe {
            (
                &mut *(&raw mut (*ptr).pool).cast::<MaybeUninit<_>>(),
                &mut *(&raw mut (*ptr).next_seq).cast::<MaybeUninit<_>>(),
            )
        };
        ObjectPool::<Timer, N>::init(pool);
        next_seq.write(Cell::new(0u64));
        // SAFETY: every field was written above.
        unsafe { place.assume_init_ref() }
    }

    pub(crate) fn alloc(&self, deadline: Instant, waker: Waker) -> TimerIndex {
        let Some((index, timer)) = self.pool.alloc() else {
            panic!("more than {N} concurrent sleeps");
        };
        let seq = self.next_seq.get();
        self.next_seq.set(seq + 1);
        timer.deadline.set(deadline);
        timer.seq.set(seq);
        timer.waker.set(Some(waker));
        index
    }

    pub(crate) fn set_waker(&self, index: TimerIndex, waker: &Waker) {
        let timer = self
            .pool
            .get(index)
            .expect("a pending sleep's timer is in storage");
        store_waker(&timer.waker, waker);
    }

    pub(crate) fn release(&self, index: TimerIndex) {
        if let Some(timer) = self.pool.get(index) {
            timer.waker.take();
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
            .map(|(_, timer)| timer.deadline.get())
            .min()
    }

    pub(crate) fn take_expired(&self, now: Instant) -> Option<Waker> {
        let (index, _) = self
            .pool
            .iter()
            .filter(|(_, timer)| timer.deadline.get() <= now)
            .min_by_key(|(_, timer)| (timer.deadline.get(), timer.seq.get()))?;
        let waker = self.pool.get(index)?.waker.take();
        self.pool.free(index);
        waker
    }
}

#[cfg(test)]
mod tests {
    use core::{cell::Cell, mem::MaybeUninit, task::Waker, time::Duration};

    use crate::{
        instant::Instant,
        storage::Timers,
        test_utils::{assert_yields, counting_waker, waker},
    };

    #[track_caller]
    fn assert_expires<const N: usize>(
        timers: &Timers<N>,
        now: Instant,
        ids: impl IntoIterator<Item = usize>,
    ) {
        let expired = core::iter::from_fn(|| timers.take_expired(now));
        assert_yields(expired.map(|waker| waker.data().addr()), ids);
    }

    #[test]
    fn alloc_and_release() {
        let now = Instant::now();
        let drops = Cell::new(0);
        let mut place = MaybeUninit::uninit();
        let timers = Timers::<2>::init(&mut place);
        let index = timers.alloc(now, counting_waker(&drops));
        assert_eq!(timers.next_deadline(), Some(now));

        timers.release(index);
        assert_eq!(drops.get(), 1);
        assert_eq!(timers.next_deadline(), None);
        timers.release(index);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    #[should_panic(expected = "more than 1 concurrent sleeps")]
    fn capacity_is_bounded() {
        let now = Instant::now();
        let mut place = MaybeUninit::uninit();
        let timers = Timers::<1>::init(&mut place);
        timers.alloc(now, Waker::noop().clone());
        timers.alloc(now, Waker::noop().clone());
    }

    #[test]
    fn expire_wakes_due_timers_in_deadline_order() {
        let earliest = Instant::now();
        let now = earliest + Duration::from_millis(1);
        let mut place = MaybeUninit::uninit();
        let timers = Timers::<8>::init(&mut place);
        timers.alloc(now + Duration::from_millis(5), waker(3));
        timers.alloc(now, waker(2));
        timers.alloc(now + Duration::from_millis(5), waker(4));
        timers.alloc(now + Duration::from_millis(10), waker(5));
        timers.alloc(earliest, waker(1));

        assert_expires(timers, now + Duration::from_millis(5), [1, 2, 3, 4]);
        assert_expires(timers, now + Duration::from_millis(9), []);
        assert_eq!(
            timers.next_deadline(),
            Some(now + Duration::from_millis(10))
        );
    }

    #[test]
    fn equal_deadlines_expire_in_insertion_order_across_slot_reuse() {
        let now = Instant::now();
        let mut place = MaybeUninit::uninit();
        let timers = Timers::<2>::init(&mut place);
        let first = timers.alloc(now, Waker::noop().clone());
        timers.alloc(now, waker(1));
        timers.release(first);
        timers.alloc(now, waker(2));
        assert_expires(timers, now, [1, 2]);
    }

    #[test]
    fn released_timer_is_not_woken() {
        let now = Instant::now();
        let mut place = MaybeUninit::uninit();
        let timers = Timers::<1>::init(&mut place);
        let index = timers.alloc(now, waker(1));
        timers.release(index);
        assert_expires(timers, now, []);
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn replaced_waker_is_the_one_woken() {
        let now = Instant::now();
        let mut place = MaybeUninit::uninit();
        let timers = Timers::<1>::init(&mut place);
        let index = timers.alloc(now, waker(1));
        timers.set_waker(index, &waker(2));
        assert_expires(timers, now, [2]);
    }

    #[test]
    fn next_deadline_is_the_earliest_one() {
        let now = Instant::now();
        let mut place = MaybeUninit::uninit();
        let timers = Timers::<4>::init(&mut place);
        assert_eq!(timers.next_deadline(), None);
        let later = now + Duration::from_millis(30);
        let earlier = now + Duration::from_millis(10);
        timers.alloc(later, Waker::noop().clone());
        let earliest = timers.alloc(earlier, Waker::noop().clone());
        timers.alloc(now + Duration::from_millis(20), Waker::noop().clone());
        assert_eq!(timers.next_deadline(), Some(earlier));
        timers.release(earliest);
        assert_eq!(
            timers.next_deadline(),
            Some(now + Duration::from_millis(20))
        );
    }
}
