use core::{cell::RefCell, mem::MaybeUninit, time::Duration};

use rustix::{
    event::kqueue::{Event, EventFilter, EventFlags, kevent, kqueue},
    fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd},
    io::{Errno, FdFlags, fcntl_setfd},
};

use crate::{
    instant::Instant,
    selectors::{
        Direction, EVENTS_PER_CALL, IoAction, IoChange, SelectorEvent, Token,
        timer::{Timer, TimerChange},
    },
};

const TIMER_IDENT: isize = 0;
const CHANGES_PER_CALL: usize = if cfg!(test) { 4 } else { 64 };

pub struct KqueueSelector {
    kq: OwnedFd,
    timer: RefCell<Timer>,
}

impl KqueueSelector {
    pub(crate) fn new() -> rustix::io::Result<Self> {
        let kq = kqueue()?;
        fcntl_setfd(&kq, FdFlags::CLOEXEC)?;
        Ok(Self {
            kq,
            timer: RefCell::new(Timer::default()),
        })
    }

    pub(crate) fn apply_now(&self, change: IoChange) -> rustix::io::Result<()> {
        self.submit([change.kevent()])
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
            .map(IoChange::kevent)
            .chain(timer_change.map(TimerChange::kevent));
        self.submit(changes)
    }

    pub(crate) fn poll_nonblocking(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
    ) -> rustix::io::Result<bool> {
        let mut buf = [MaybeUninit::<Event>::uninit(); EVENTS_PER_CALL];
        loop {
            // SAFETY: the changelist is empty, so this call doesn't register any fd.
            let result = unsafe { kevent(&self.kq, &[], &mut buf, Some(Duration::ZERO)) };
            match result {
                Ok((events, _)) => {
                    let full = events.len() == EVENTS_PER_CALL;
                    let mut translated = events.iter().filter_map(|event| self.consume(event));
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
        let mut list = [TimerChange::Disarm.kevent(); CHANGES_PER_CALL];
        let mut receipts = [MaybeUninit::<Event>::uninit(); CHANGES_PER_CALL];
        loop {
            let mut len = 0;
            for (entry, change) in list.iter_mut().zip(&mut changes) {
                *entry = change;
                len += 1;
            }
            if len == 0 {
                return Ok(());
            }
            self.submit_list(&list[..len], &mut receipts)?;
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
        let result = unsafe { kevent(&self.kq, changes, receipts, Some(Duration::ZERO)) };
        let receipts = match result {
            Ok((receipts, _)) => receipts,
            Err(Errno::INTR) => return Ok(()),
            Err(errno) => return Err(errno),
        };
        for receipt in receipts.iter() {
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
        match event.filter() {
            EventFilter::Read(_) => Some(SelectorEvent::Readable(token)),
            EventFilter::Write(_) => Some(SelectorEvent::Writable(token)),
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

impl IoChange {
    fn kevent(self) -> Event {
        let filter = match self.direction {
            Direction::Read => EventFilter::Read(self.fd),
            Direction::Write => EventFilter::Write(self.fd),
        };
        let flags = match self.action {
            IoAction::Arm => EventFlags::ADD | EventFlags::ONESHOT,
            IoAction::Disarm => EventFlags::DELETE,
        };
        let udata = core::ptr::without_provenance_mut(self.token.0 as usize);
        Event::new(filter, flags | EventFlags::RECEIPT, udata)
    }
}

impl AsFd for KqueueSelector {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.kq.as_fd()
    }
}

impl AsRawFd for KqueueSelector {
    fn as_raw_fd(&self) -> RawFd {
        self.kq.as_raw_fd()
    }
}
