use core::{cell::RefCell, ffi::c_void, mem::MaybeUninit, time::Duration};

use rustix::{
    event::kqueue::{Event, EventFilter, EventFlags, kevent, kqueue},
    fd::{AsFd, BorrowedFd, OwnedFd},
    io::{Errno, FdFlags, fcntl_setfd},
};

use crate::{
    selector::{
        Direction, Directions, EVENTS_PER_CALL, IoChange, KernelTimer, SelectorEvent, TimerChange,
    },
    strace::{syscall, syscall_end, syscall_start},
    time::Instant,
};

const TIMER_IDENT: isize = 0;
const CHANGES_PER_CALL: usize = if cfg!(test) { 4 } else { 64 };

pub(crate) struct KqueueSelector {
    kq: OwnedFd,
    timer: RefCell<KernelTimer>,
}

impl KqueueSelector {
    pub(crate) fn new() -> rustix::io::Result<Self> {
        syscall_start!("kqueue");
        let kq = kqueue();
        syscall_end!(kq);
        let kq = kq?;
        syscall_start!("fcntl", kq, "F_SETFD", FdFlags::CLOEXEC);
        let result = fcntl_setfd(&kq, FdFlags::CLOEXEC);
        syscall_end!(result);
        result?;
        Ok(Self {
            kq,
            timer: RefCell::new(KernelTimer::default()),
        })
    }

    pub(crate) fn apply_now(&self, change: IoChange) -> rustix::io::Result<()> {
        self.submit(Self::io_kevents(change))
    }

    pub(crate) fn arm_timer(&self, deadline: Instant) {
        self.timer.borrow_mut().arm_in_memory(deadline);
    }

    pub(crate) fn disarm_timer(&self) {
        self.timer.borrow_mut().disarm_in_memory();
    }

    pub(crate) fn flush(
        &self,
        io_changes: impl IntoIterator<Item = IoChange>,
    ) -> rustix::io::Result<()> {
        let timer_change = self.timer.borrow_mut().flush(Instant::now());
        let changes = io_changes
            .into_iter()
            .flat_map(Self::io_kevents)
            .chain(timer_change.map(Self::timer_kevent));
        self.submit(changes)
    }

    pub(crate) fn poll_nonblocking(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
    ) -> rustix::io::Result<bool> {
        let mut buf = [MaybeUninit::<Event>::uninit(); EVENTS_PER_CALL];
        loop {
            // SAFETY: the changelist is empty, so this call doesn't register any fd.
            syscall_start!("kevent", self.kq, &[][..] as &[Event]);
            let result = unsafe { kevent(&self.kq, &[], &mut buf, Some(Duration::ZERO)) };
            syscall_end!(
                result;
                result.as_ref().ok().map(|(events, _)| &**events),
                EVENTS_PER_CALL,
                Duration::ZERO,
            );
            match result {
                Ok((events, _)) => {
                    let full = events.len() == EVENTS_PER_CALL;
                    let mut translated = events.iter().map(|event| self.consume(event));
                    out.fill_with(|| translated.next());
                    return Ok(full);
                }
                Err(Errno::INTR) => continue,
                Err(errno) => return Err(errno),
            }
        }
    }

    fn submit(&self, changes: impl IntoIterator<Item = Event>) -> rustix::io::Result<()> {
        let mut changes = changes.into_iter();
        let mut list = [Self::timer_kevent(TimerChange::Disarm); CHANGES_PER_CALL];
        let mut receipts = [MaybeUninit::<Event>::uninit(); CHANGES_PER_CALL];
        loop {
            let mut len = 0;
            for (filled, (entry, change)) in (1..).zip(list.iter_mut().zip(&mut changes)) {
                *entry = change;
                len = filled;
            }
            if len == 0 {
                return Ok(());
            }
            let Some(filled) = list.get(..len) else {
                unreachable!("at most {CHANGES_PER_CALL} changes are filled");
            };
            self.submit_list(filled, &mut receipts)?;
            if len < CHANGES_PER_CALL {
                return Ok(());
            }
        }
    }

    fn submit_list(
        &self,
        changes: &[Event],
        receipts: &mut [MaybeUninit<Event>; CHANGES_PER_CALL],
    ) -> rustix::io::Result<()> {
        // SAFETY: the callers (`flush`, `apply_now`) only pass open fds, and a filter is deleted
        // (`apply_now`) before its registration ends and the fd can be closed, so the kqueue
        // never refers to an invalid fd.
        syscall_start!("kevent", self.kq, changes);
        let result = unsafe { kevent(&self.kq, changes, receipts, Some(Duration::ZERO)) };
        syscall_end!(
            result;
            result.as_ref().ok().map(|(receipts, _)| &**receipts),
            CHANGES_PER_CALL,
            Duration::ZERO,
        );
        let receipts = match result {
            Ok((receipts, _)) => receipts,
            Err(Errno::INTR) => return Ok(()),
            Err(errno) => return Err(errno),
        };
        for receipt in receipts.iter() {
            match receipt.data() {
                0 => {}
                errno if errno == i64::from(Errno::NOENT.raw_os_error()) => {}
                errno => {
                    return Err(i32::try_from(errno).map_or(Errno::INVAL, Errno::from_raw_os_error));
                }
            }
        }
        Ok(())
    }

    fn consume(&self, event: &Event) -> SelectorEvent {
        let token = event.udata().addr();
        match event.filter() {
            EventFilter::Read(_) => SelectorEvent::Io {
                token,
                ready: Directions::Read,
            },
            EventFilter::Write(_) => SelectorEvent::Io {
                token,
                ready: Directions::Write,
            },
            EventFilter::Timer { .. } => {
                self.timer.borrow_mut().fired();
                SelectorEvent::Timer
            }
            _ => unreachable!("only read, write and timer filters are registered"),
        }
    }

    fn timer_kevent(change: TimerChange) -> Event {
        let (timer, flags) = match change {
            TimerChange::Arm(after) => (
                Some(round_up_to_millis(after)),
                EventFlags::ADD | EventFlags::ONESHOT,
            ),
            TimerChange::Disarm => (None, EventFlags::DELETE),
        };
        Event::new(
            EventFilter::Timer {
                ident: TIMER_IDENT,
                timer,
            },
            flags | EventFlags::RECEIPT,
            core::ptr::null_mut(),
        )
    }

    fn io_kevents(change: IoChange) -> [Event; 2] {
        let udata = core::ptr::without_provenance_mut(change.token());
        [
            Self::io_kevent(
                EventFilter::Read(change.fd()),
                change.interest().contains(Direction::Read),
                udata,
            ),
            Self::io_kevent(
                EventFilter::Write(change.fd()),
                change.interest().contains(Direction::Write),
                udata,
            ),
        ]
    }

    fn io_kevent(filter: EventFilter, armed: bool, udata: *mut c_void) -> Event {
        let flags = if armed {
            EventFlags::ADD | EventFlags::ONESHOT
        } else {
            EventFlags::DELETE
        };
        Event::new(filter, flags | EventFlags::RECEIPT, udata)
    }
}

impl Drop for KqueueSelector {
    fn drop(&mut self) {
        syscall!("close", self.kq);
    }
}

impl AsFd for KqueueSelector {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.kq.as_fd()
    }
}

fn round_up_to_millis(d: Duration) -> Duration {
    let millis = d.as_nanos().div_ceil(1_000_000);
    Duration::from_millis(millis.try_into().unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use crate::selector::kqueue::round_up_to_millis;

    #[test]
    fn timer_durations_round_up_to_whole_millis() {
        assert_eq!(
            round_up_to_millis(Duration::from_micros(1500)),
            Duration::from_millis(2)
        );
    }
}
