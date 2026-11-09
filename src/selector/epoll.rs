use core::{cell::RefCell, time::Duration};

use rustix::{
    event::{
        Timespec,
        epoll::{self, CreateFlags, Event, EventData, EventFlags},
    },
    fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd},
    io::Errno,
    time::{
        Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, timerfd_create,
        timerfd_settime,
    },
};

use crate::{
    selector::{
        Direction, EVENTS_PER_CALL, IoAction, IoChange, KernelTimer, SelectorEvent, TimerChange,
        Token,
    },
    time::Instant,
};

const TIMER_DATA: u64 = u64::MAX;
const ZERO: Timespec = Timespec {
    tv_sec: 0,
    tv_nsec: 0,
};
const NO_EVENT: Event = Event {
    flags: EventFlags::empty(),
    data: EventData::new_u64(0),
};

pub struct EpollSelector {
    epoll: OwnedFd,
    timer_fd: OwnedFd,
    timer: RefCell<KernelTimer>,
}

impl EpollSelector {
    pub fn new() -> rustix::io::Result<Self> {
        let epoll = epoll::create(CreateFlags::CLOEXEC)?;
        let timer_fd = timerfd_create(
            TimerfdClockId::Monotonic,
            TimerfdFlags::NONBLOCK | TimerfdFlags::CLOEXEC,
        )?;
        epoll::add(
            &epoll,
            &timer_fd,
            EventData::new_u64(TIMER_DATA),
            EventFlags::IN,
        )?;
        Ok(Self {
            epoll,
            timer_fd,
            timer: RefCell::new(KernelTimer::default()),
        })
    }

    pub(crate) fn apply_now(&self, change: IoChange) -> rustix::io::Result<()> {
        self.apply_io(change)
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
        for change in io_changes {
            self.apply_io(change)?;
        }
        let timer_change = self.timer.borrow_mut().flush(Instant::now());
        match timer_change {
            Some(change) => self.apply_timer(change),
            None => Ok(()),
        }
    }

    pub(crate) fn poll_nonblocking(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
    ) -> rustix::io::Result<bool> {
        let mut buf = [NO_EVENT; EVENTS_PER_CALL];
        let len = loop {
            match epoll::wait(&self.epoll, &mut buf, Some(&ZERO)) {
                Ok(len) => break len,
                Err(Errno::INTR) => continue,
                Err(errno) => return Err(errno),
            }
        };
        out.fill(None);
        let mut count = 0;
        for event in buf[..len].iter().copied() {
            if let Some(translated) = self.consume(event)? {
                out[count] = Some(translated);
                count += 1;
            }
        }
        Ok(len == EVENTS_PER_CALL)
    }

    fn consume(&self, event: Event) -> rustix::io::Result<Option<SelectorEvent>> {
        match event.data.u64() {
            TIMER_DATA => self.consume_timer(),
            token => Ok(Some(SelectorEvent::Io(Token(token)))),
        }
    }

    fn consume_timer(&self) -> rustix::io::Result<Option<SelectorEvent>> {
        let mut expirations = [0; 8];
        match rustix::io::read(&self.timer_fd, &mut expirations) {
            Ok(_) => {
                self.timer.borrow_mut().fired();
                Ok(Some(SelectorEvent::Timer))
            }
            Err(Errno::AGAIN) => Ok(None),
            Err(errno) => Err(errno),
        }
    }

    fn apply_io(&self, change: IoChange) -> rustix::io::Result<()> {
        // SAFETY: the callers (`flush`, `apply_now`) only pass fds that stay open for the
        // duration of this call.
        let fd = unsafe { BorrowedFd::borrow_raw(change.fd) };
        match change.action {
            IoAction::Arm => {
                let interest = match change.direction {
                    Direction::Read => EventFlags::IN,
                    Direction::Write => EventFlags::OUT,
                };
                let data = EventData::new_u64(change.token.0);
                let flags = interest | EventFlags::ONESHOT;
                match epoll::modify(&self.epoll, fd, data, flags) {
                    Err(Errno::NOENT) => epoll::add(&self.epoll, fd, data, flags),
                    result => result,
                }
            }
            IoAction::Disarm => match epoll::delete(&self.epoll, fd) {
                Err(Errno::NOENT) => Ok(()),
                result => result,
            },
        }
    }

    fn apply_timer(&self, change: TimerChange) -> rustix::io::Result<()> {
        let it_value = match change {
            TimerChange::Arm(after) => Timespec::try_from(after.max(Duration::from_nanos(1)))
                .unwrap_or(Timespec {
                    tv_sec: i64::MAX,
                    tv_nsec: 0,
                }),
            TimerChange::Disarm => ZERO,
        };
        let spec = Itimerspec {
            it_interval: ZERO,
            it_value,
        };
        timerfd_settime(&self.timer_fd, TimerfdTimerFlags::empty(), &spec)?;
        Ok(())
    }
}

impl AsFd for EpollSelector {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.epoll.as_fd()
    }
}

impl AsRawFd for EpollSelector {
    fn as_raw_fd(&self) -> RawFd {
        self.epoll.as_raw_fd()
    }
}
