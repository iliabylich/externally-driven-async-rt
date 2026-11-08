use core::{alloc::Layout, cell::Cell, ptr::NonNull, task::Waker};

use crate::{
    instant::Instant,
    pool::{Index, ObjectPool},
    storage::store_waker,
};

// Pool values are `Cell`s (see `ObjectPool`); a vacant slot holds a stale or default value.
#[derive(Default)]
pub(crate) struct Timer {
    deadline: Cell<Instant>,
    // Insertion order, so timers with equal deadlines expire in the order they were created.
    seq: Cell<u64>,
    waker: Cell<Option<Waker>>,
}

pub(crate) type TimerIndex = Index<Timer>;

pub(crate) struct Timers {
    pool: &'static ObjectPool<Timer>,
    next_seq: Cell<u64>,
}

impl Timers {
    pub(crate) fn layout(len: usize) -> Layout {
        ObjectPool::<Timer>::layout(len)
    }

    /// # Safety
    ///
    /// As `ObjectPool::init`, with the memory valid (and otherwise unused) forever.
    pub(crate) unsafe fn init(ptr: NonNull<u8>, len: usize) -> Self {
        Self {
            // SAFETY: guaranteed by the caller.
            pool: unsafe { ObjectPool::init(ptr, len) },
            next_seq: Cell::new(0),
        }
    }

    pub(crate) fn alloc(&self, deadline: Instant, waker: Waker) -> TimerIndex {
        let Some((index, timer)) = self.pool.alloc() else {
            panic!("more than {} concurrent sleeps", self.pool.capacity());
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

    /// Releases every timer, so the pool holds nothing that needs dropping.
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
        test_utils::{aligned_in, assert_yields, counting_waker, stack_memory, waker},
    };

    // No cleanup needed: the tests' wakers do nothing when dropped, or are released explicitly.
    fn timers_in(memory: &mut [MaybeUninit<u8>], len: usize) -> Timers {
        let ptr = aligned_in(memory, Timers::layout(len));
        // SAFETY: `ptr` is aligned and valid for the layout, and every test keeps `memory` alive
        // for as long as the returned timers.
        unsafe { Timers::init(ptr, len) }
    }

    #[track_caller]
    fn assert_expires(timers: &Timers, now: Instant, ids: impl IntoIterator<Item = usize>) {
        let expired = core::iter::from_fn(|| timers.take_expired(now));
        assert_yields(expired.map(|waker| waker.data().addr()), ids);
    }

    #[test]
    fn alloc_and_release() {
        let now = Instant::now();
        let drops = Cell::new(0);
        let mut memory = stack_memory();
        let timers = timers_in(&mut memory, 2);
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
        let mut memory = stack_memory();
        let timers = timers_in(&mut memory, 1);
        timers.alloc(now, Waker::noop().clone());
        timers.alloc(now, Waker::noop().clone());
    }

    #[test]
    fn expire_wakes_due_timers_in_deadline_order() {
        let earliest = Instant::now();
        let now = earliest + Duration::from_millis(1);
        let mut memory = stack_memory();
        let timers = timers_in(&mut memory, 8);
        timers.alloc(now + Duration::from_millis(5), waker(3));
        timers.alloc(now, waker(2));
        timers.alloc(now + Duration::from_millis(5), waker(4));
        timers.alloc(now + Duration::from_millis(10), waker(5));
        timers.alloc(earliest, waker(1));

        assert_expires(&timers, now + Duration::from_millis(5), [1, 2, 3, 4]);
        assert_expires(&timers, now + Duration::from_millis(9), []);
        assert_eq!(
            timers.next_deadline(),
            Some(now + Duration::from_millis(10))
        );
    }

    #[test]
    fn equal_deadlines_expire_in_insertion_order_across_slot_reuse() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let timers = timers_in(&mut memory, 2);
        let first = timers.alloc(now, Waker::noop().clone());
        timers.alloc(now, waker(1));
        timers.release(first);
        timers.alloc(now, waker(2));
        assert_expires(&timers, now, [1, 2]);
    }

    #[test]
    fn released_timer_is_not_woken() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let timers = timers_in(&mut memory, 1);
        let index = timers.alloc(now, waker(1));
        timers.release(index);
        assert_expires(&timers, now, []);
        assert_eq!(timers.next_deadline(), None);
    }

    #[test]
    fn replaced_waker_is_the_one_woken() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let timers = timers_in(&mut memory, 1);
        let index = timers.alloc(now, waker(1));
        timers.set_waker(index, &waker(2));
        assert_expires(&timers, now, [2]);
    }

    #[test]
    fn next_deadline_is_the_earliest_one() {
        let now = Instant::now();
        let mut memory = stack_memory();
        let timers = timers_in(&mut memory, 4);
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
