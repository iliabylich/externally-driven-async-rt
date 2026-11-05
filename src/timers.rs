use alloc::collections::BTreeMap;
use core::task::Waker;
use core::time::Duration;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TimerKey {
    deadline: Instant,
    id: u64,
}

#[derive(Default)]
pub(crate) struct Timers {
    map: BTreeMap<TimerKey, Waker>,
    next_id: u64,
}

impl Timers {
    pub(crate) fn insert(&mut self, deadline: Instant, waker: Waker) -> TimerKey {
        let key = TimerKey {
            deadline,
            id: self.next_id,
        };
        self.next_id += 1;
        self.map.insert(key, waker);
        key
    }

    pub(crate) fn update_waker(&mut self, key: TimerKey, waker: &Waker) {
        if let Some(stored) = self.map.get_mut(&key) {
            stored.clone_from(waker);
        }
    }

    pub(crate) fn remove(&mut self, key: TimerKey) {
        self.map.remove(&key);
    }

    pub(crate) fn expire(&mut self, now: Instant, out: &mut Vec<Waker>) {
        while let Some(entry) = self.map.first_entry() {
            if entry.key().deadline > now {
                return;
            }
            out.push(entry.remove());
        }
    }

    pub(crate) fn next_timeout(&self, now: Instant) -> Option<Duration> {
        let (key, _) = self.map.first_key_value()?;
        Some(key.deadline.saturating_duration_since(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::sync::Arc;
    use std::sync::Mutex;
    use std::task::Wake;

    #[derive(Default)]
    struct Log(Mutex<Vec<u32>>);

    struct Recorder {
        id: u32,
        log: Arc<Log>,
    }

    impl Wake for Recorder {
        fn wake(self: Arc<Self>) {
            self.log.0.lock().unwrap().push(self.id);
        }
    }

    fn waker(log: &Arc<Log>, id: u32) -> Waker {
        Waker::from(Arc::new(Recorder {
            id,
            log: log.clone(),
        }))
    }

    fn wake_all(wakers: Vec<Waker>, log: &Arc<Log>) -> Vec<u32> {
        wakers.into_iter().for_each(Waker::wake);
        core::mem::take(&mut *log.0.lock().unwrap())
    }

    #[test]
    fn expire_wakes_due_timers_in_deadline_order() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let mut timers = Timers::default();
        timers.insert(now + Duration::from_millis(5), waker(&log, 3));
        timers.insert(now, waker(&log, 2));
        timers.insert(now + Duration::from_millis(5), waker(&log, 4));
        timers.insert(now + Duration::from_millis(10), waker(&log, 5));
        timers.insert(now - Duration::from_millis(1), waker(&log, 1));

        let mut out = Vec::new();
        timers.expire(now + Duration::from_millis(5), &mut out);
        assert_eq!(wake_all(out, &log), vec![1, 2, 3, 4]);

        let mut out = Vec::new();
        timers.expire(now + Duration::from_millis(9), &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn removed_timer_is_not_woken() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let mut timers = Timers::default();
        let key = timers.insert(now, waker(&log, 1));
        timers.remove(key);
        let mut out = Vec::new();
        timers.expire(now, &mut out);
        assert!(out.is_empty());
        assert_eq!(timers.next_timeout(now), None);
    }

    #[test]
    fn update_waker_replaces_the_waker() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let mut timers = Timers::default();
        let key = timers.insert(now, waker(&log, 1));
        timers.update_waker(key, &waker(&log, 2));
        let mut out = Vec::new();
        timers.expire(now, &mut out);
        assert_eq!(wake_all(out, &log), vec![2]);
    }

    #[test]
    fn next_timeout_is_the_earliest_deadline() {
        let log = Arc::new(Log::default());
        let now = Instant::now();
        let mut timers = Timers::default();
        assert_eq!(timers.next_timeout(now), None);
        timers.insert(now + Duration::from_millis(30), waker(&log, 1));
        let earliest = timers.insert(now + Duration::from_millis(10), waker(&log, 2));
        assert_eq!(timers.next_timeout(now), Some(Duration::from_millis(10)));
        timers.remove(earliest);
        assert_eq!(timers.next_timeout(now), Some(Duration::from_millis(30)));
        assert_eq!(
            timers.next_timeout(now + Duration::from_secs(1)),
            Some(Duration::ZERO)
        );
    }
}
