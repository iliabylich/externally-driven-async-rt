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
// Small in tests, so a handful of fds exercises submitting more than one changelist.
const CHANGES_PER_CALL: usize = if cfg!(test) { 4 } else { 64 };

pub(crate) struct Selector {
    kq: OwnedFd,
    timer: RefCell<Timer>,
}

impl Selector {
    pub(crate) fn new() -> rustix::io::Result<Self> {
        let kq = kqueue()?;
        fcntl_setfd(&kq, FdFlags::CLOEXEC)?;
        Ok(Self {
            kq,
            timer: RefCell::new(Timer::default()),
        })
    }

    /// Applies `change` right away instead of in the next `flush`. Used to delete the filter of
    /// a registration that ends: by the next `flush` its fd may be closed and its number reused
    /// by another socket, whose filter a batched delete would remove. `change.fd` must be open.
    pub(crate) fn apply_now(&self, change: IoChange) -> rustix::io::Result<()> {
        self.submit([change.kevent()])
    }

    pub(crate) fn arm_timer(&self, deadline: Instant) {
        self.timer.borrow_mut().arm_in_memory(deadline);
    }

    pub(crate) fn disarm_timer(&self) {
        self.timer.borrow_mut().disarm_in_memory();
    }

    /// Submits `io_changes` and the pending timer change, in changelists of up to
    /// `CHANGES_PER_CALL` entries. Every fd in `io_changes` must be open.
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

    /// Fills `out` with one batch of events, followed by `None`s. Returns whether the batch was
    /// full, i.e. more events may be pending.
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
        // Only the first `len` entries are ever submitted, the filler is never read.
        let mut list = [TimerChange::Disarm.kevent(); CHANGES_PER_CALL];
        let mut receipts = [MaybeUninit::<Event>::uninit(); CHANGES_PER_CALL];
        loop {
            let mut len = 0;
            // `zip` pulls from `list` first, so no change is taken without a free entry for it.
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
            // kevent applies the whole changelist before it can be interrupted (documented on
            // FreeBSD, relied upon by mio), so EINTR here means the changes are in place.
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
