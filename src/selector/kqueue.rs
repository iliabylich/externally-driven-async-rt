use core::{cell::Cell, mem::MaybeUninit, time::Duration};

use rustix::{
    event::kqueue::{Event, EventFilter, EventFlags, UserDefinedFlags, UserFlags, kevent, kqueue},
    fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd},
    io::{Errno, FdFlags, fcntl_setfd},
};

use crate::{
    selector::{Directions, EVENTS_PER_CALL, KernelTimer, SelectorEvent, TimerChange},
    strace::{syscall, syscall_end, syscall_start},
    time::Instant,
};

const TIMER_IDENT: isize = 0;
const NOTIFY_IDENT: isize = 0;
const MAX_CHANGES: usize = 2;

#[must_use]
pub(crate) struct KqueueSelector {
    kq: OwnedFd,
    timer: Cell<KernelTimer>,
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
        let selector = Self {
            kq,
            timer: Cell::new(KernelTimer::default()),
        };
        selector.submit(&[Self::notify_kevent(
            UserFlags::empty(),
            EventFlags::ADD | EventFlags::CLEAR,
        )])?;
        Ok(selector)
    }

    pub(crate) fn notify(&self) -> rustix::io::Result<()> {
        self.submit(&[Self::notify_kevent(UserFlags::TRIGGER, EventFlags::empty())])
    }

    pub(crate) fn register(&self, fd: BorrowedFd<'_>) -> rustix::io::Result<()> {
        self.submit(&Self::io_kevents(fd, EventFlags::ADD | EventFlags::CLEAR))
    }

    pub(crate) fn deregister(&self, fd: BorrowedFd<'_>) -> rustix::io::Result<()> {
        self.submit(&Self::io_kevents(fd, EventFlags::DELETE))
    }

    pub(crate) fn flush(&self, deadline: Option<Instant>, now: Instant) -> rustix::io::Result<()> {
        let (timer, timer_change) = self.timer.get().flush(deadline, now);
        self.timer.set(timer);
        let Some(change) = timer_change else {
            return Ok(());
        };
        self.submit(&[Self::timer_kevent(change)])
    }

    pub(crate) fn poll(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
        timeout: Option<Duration>,
    ) -> rustix::io::Result<bool> {
        let mut buf = [MaybeUninit::<Event>::uninit(); EVENTS_PER_CALL];
        loop {
            // SAFETY: the changelist is empty, so this call doesn't register any fd.
            syscall_start!("kevent", self.kq, &[][..] as &[Event]);
            let result = unsafe { kevent(&self.kq, &[], &mut buf, timeout) };
            syscall_end!(
                result;
                result.as_ref().ok().map(|(events, _)| &**events),
                EVENTS_PER_CALL,
                timeout,
            );
            match result {
                Ok((events, _)) => {
                    let full = events.len() == EVENTS_PER_CALL;
                    let mut translated = events.iter().map(|event| self.consume(event));
                    out.fill_with(|| translated.next());
                    return Ok(full);
                }
                Err(Errno::INTR) => {}
                Err(errno) => return Err(errno),
            }
        }
    }

    fn submit(&self, changes: &[Event]) -> rustix::io::Result<()> {
        let mut receipts = [MaybeUninit::<Event>::uninit(); MAX_CHANGES];
        // SAFETY: only `register` and `deregister` pass fds, and only open ones, and every registered
        // fd is deregistered before it's closed, so the kqueue never refers to an invalid fd.
        syscall_start!("kevent", self.kq, changes);
        let result = unsafe { kevent(&self.kq, changes, &mut receipts, Some(Duration::ZERO)) };
        syscall_end!(
            result;
            result.as_ref().ok().map(|(receipts, _)| &**receipts),
            MAX_CHANGES,
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
        match event.filter() {
            EventFilter::Read(fd) => SelectorEvent::Io {
                fd,
                ready: Directions::Read,
            },
            EventFilter::Write(fd) => SelectorEvent::Io {
                fd,
                ready: Directions::Write,
            },
            EventFilter::Timer { .. } => {
                self.timer.set(KernelTimer::Disarmed);
                SelectorEvent::Timer
            }
            EventFilter::User { .. } => SelectorEvent::Notify,
            _ => unreachable!("only read, write, timer and user filters are registered"),
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

    fn notify_kevent(flags: UserFlags, event_flags: EventFlags) -> Event {
        Event::new(
            EventFilter::User {
                ident: NOTIFY_IDENT,
                flags,
                user_flags: UserDefinedFlags::new(0),
            },
            event_flags | EventFlags::RECEIPT,
            core::ptr::null_mut(),
        )
    }

    fn io_kevents(fd: BorrowedFd<'_>, flags: EventFlags) -> [Event; 2] {
        let fd = fd.as_raw_fd();
        [EventFilter::Read(fd), EventFilter::Write(fd)]
            .map(|filter| Event::new(filter, flags | EventFlags::RECEIPT, core::ptr::null_mut()))
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
