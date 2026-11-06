use core::cell::RefCell;
use core::mem::MaybeUninit;
use core::time::Duration;
use std::time::Instant;

use rustix::buffer::spare_capacity;
use rustix::event::kqueue::{Event, EventFilter, EventFlags, kevent, kqueue};
use rustix::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use rustix::io::{Errno, FdFlags, fcntl_setfd};

use super::registrations::{Registrations, Transition};
use super::timer::{Timer, TimerChange};
use super::{Interest, SelectorEvent, Token};

const TIMER_IDENT: isize = 0;
const EVENTS_PER_CALL: usize = 64;

pub(crate) struct Selector {
    kq: OwnedFd,
    registrations: RefCell<Registrations>,
    timer: RefCell<Timer>,
}

impl Selector {
    pub(crate) fn new() -> rustix::io::Result<Self> {
        let kq = kqueue()?;
        fcntl_setfd(&kq, FdFlags::CLOEXEC)?;
        Ok(Self {
            kq,
            registrations: RefCell::new(Registrations::default()),
            timer: RefCell::new(Timer::default()),
        })
    }

    pub(crate) fn register(&self, fd: BorrowedFd<'_>) -> Token {
        self.registrations
            .borrow_mut()
            .register_in_memory(fd.as_raw_fd())
    }

    /// Must be called while the fd is still open. Armed filters are deleted right away (not
    /// in the next `flush`): by then the fd may be closed and its number reused by another
    /// socket, whose filters a batched delete would remove.
    pub(crate) fn deregister(&self, token: Token) -> rustix::io::Result<()> {
        let Some(transition) = self.registrations.borrow_mut().deregister_now(token) else {
            return Ok(());
        };
        let changes: Vec<Event> = transition.kevents().collect();
        self.submit(&changes)
    }

    pub(crate) fn set_interest(&self, token: Token, interest: Interest) {
        self.registrations
            .borrow_mut()
            .set_interest_in_memory(token, interest);
    }

    pub(crate) fn arm_timer(&self, deadline: Instant) {
        self.timer.borrow_mut().arm_in_memory(deadline);
    }

    pub(crate) fn disarm_timer(&self) {
        self.timer.borrow_mut().disarm_in_memory();
    }

    pub(crate) fn flush(&self) -> rustix::io::Result<()> {
        let mut changes = Vec::new();
        for transition in self.registrations.borrow_mut().flush() {
            changes.extend(transition.kevents());
        }
        if let Some(change) = self.timer.borrow_mut().flush(Instant::now()) {
            changes.push(change.kevent());
        }
        self.submit(&changes)
    }

    pub(crate) fn poll_nonblocking(&self, out: &mut Vec<SelectorEvent>) -> rustix::io::Result<()> {
        let mut buf = [MaybeUninit::<Event>::uninit(); EVENTS_PER_CALL];
        loop {
            // SAFETY: the changelist is empty, so this call doesn't register any fd.
            let result = unsafe { kevent(&self.kq, &[], &mut buf, Some(Duration::ZERO)) };
            let (events, _) = match result {
                Ok(result) => result,
                Err(Errno::INTR) => continue,
                Err(errno) => return Err(errno),
            };
            let count = events.len();
            for event in events.iter() {
                if let Some(event) = self.consume(event) {
                    out.push(event);
                }
            }
            if count < EVENTS_PER_CALL {
                return Ok(());
            }
        }
    }

    fn submit(&self, changes: &[Event]) -> rustix::io::Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut receipts = Vec::with_capacity(changes.len());
        // SAFETY: every fd in `changes` belongs to a live registration, and registrations
        // are removed (with their filters) before their fd is closed (see `deregister`),
        // so the kqueue never refers to an invalid fd.
        let result = unsafe {
            kevent(
                &self.kq,
                changes,
                spare_capacity(&mut receipts),
                Some(Duration::ZERO),
            )
        };
        match result {
            Ok(_) => {}
            // kevent applies the whole changelist before it can be interrupted (documented on
            // FreeBSD, relied upon by mio), so EINTR here means the changes are in place.
            Err(Errno::INTR) => return Ok(()),
            Err(errno) => return Err(errno),
        }
        for receipt in &receipts {
            match receipt.data() {
                0 => {}
                errno if errno == i64::from(Errno::NOENT.raw_os_error()) => {}
                errno => return Err(Errno::from_raw_os_error(errno as i32)),
            }
        }
        Ok(())
    }

    fn consume(&self, event: &Event) -> Option<SelectorEvent> {
        let token = Token(event.udata().addr() as u64);
        let mut registrations = self.registrations.borrow_mut();
        match event.filter() {
            EventFilter::Read(_) => registrations.on_readable(token),
            EventFilter::Write(_) => registrations.on_writable(token),
            EventFilter::Timer { .. } => {
                self.timer.borrow_mut().fired();
                Some(SelectorEvent::Timer)
            }
            _ => None,
        }
    }
}

impl TimerChange {
    fn kevent(self) -> Event {
        let (timer, flags) = match self {
            // `after` is whole milliseconds (see `Timer::flush`): rustix truncates
            // sub-ms parts for some durations (e.g. 1.0005s becomes 1s), but converts these
            // exactly.
            TimerChange::Arm(after) => (Some(after), EventFlags::ADD | EventFlags::ONESHOT),
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
}

impl Transition {
    fn read_flags(&self) -> Option<EventFlags> {
        filter_flags(self.armed.read, self.desired.read)
    }

    fn write_flags(&self) -> Option<EventFlags> {
        filter_flags(self.armed.write, self.desired.write)
    }

    fn kevents(&self) -> impl Iterator<Item = Event> {
        let udata = core::ptr::without_provenance_mut(self.token.0 as usize);
        let read = self
            .read_flags()
            .map(|flags| Event::new(EventFilter::Read(self.fd), flags, udata));
        let write = self
            .write_flags()
            .map(|flags| Event::new(EventFilter::Write(self.fd), flags, udata));
        read.into_iter().chain(write)
    }
}

fn filter_flags(armed: bool, desired: bool) -> Option<EventFlags> {
    match (armed, desired) {
        (false, true) => Some(EventFlags::ADD | EventFlags::ONESHOT | EventFlags::RECEIPT),
        (true, false) => Some(EventFlags::DELETE | EventFlags::RECEIPT),
        _ => None,
    }
}

impl AsFd for Selector {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.kq.as_fd()
    }
}

impl AsRawFd for Selector {
    fn as_raw_fd(&self) -> RawFd {
        self.kq.as_raw_fd()
    }
}
