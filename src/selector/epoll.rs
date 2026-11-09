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
    strace::{syscall, syscall_end, syscall_start},
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
        syscall_start!("epoll_create1", CreateFlags::CLOEXEC);
        let epoll = epoll::create(CreateFlags::CLOEXEC);
        syscall_end!(epoll);
        let epoll = epoll?;

        let timer_flags = TimerfdFlags::NONBLOCK | TimerfdFlags::CLOEXEC;
        syscall_start!("timerfd_create", TimerfdClockId::Monotonic, timer_flags);
        let timer_fd = timerfd_create(TimerfdClockId::Monotonic, timer_flags);
        syscall_end!(timer_fd);
        let timer_fd = timer_fd?;

        let data = EventData::new_u64(TIMER_DATA);
        let flags = EventFlags::IN;
        syscall_start!(
            "epoll_ctl",
            epoll,
            "EPOLL_CTL_ADD",
            timer_fd,
            Event { flags, data }
        );
        let result = epoll::add(&epoll, &timer_fd, data, flags);
        syscall_end!(result);
        result?;

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
            syscall_start!("epoll_wait", self.epoll);
            let result = epoll::wait(&self.epoll, &mut buf, Some(&ZERO));
            syscall_end!(result; &buf[..result.unwrap_or(0)], EVENTS_PER_CALL, ZERO);
            match result {
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
        syscall_start!("read", self.timer_fd);
        let result = rustix::io::read(&self.timer_fd, &mut expirations);
        syscall_end!(result; &expirations[..result.unwrap_or(0)], expirations.len());
        match result {
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
                syscall_start!(
                    "epoll_ctl",
                    self.epoll,
                    "EPOLL_CTL_MOD",
                    fd,
                    Event { flags, data }
                );
                let modified = epoll::modify(&self.epoll, fd, data, flags);
                syscall_end!(modified);
                if modified != Err(Errno::NOENT) {
                    return modified;
                }
                syscall_start!(
                    "epoll_ctl",
                    self.epoll,
                    "EPOLL_CTL_ADD",
                    fd,
                    Event { flags, data }
                );
                let added = epoll::add(&self.epoll, fd, data, flags);
                syscall_end!(added);
                added
            }
            IoAction::Disarm => {
                syscall_start!("epoll_ctl", self.epoll, "EPOLL_CTL_DEL", fd);
                let deleted = epoll::delete(&self.epoll, fd);
                syscall_end!(deleted);
                match deleted {
                    Err(Errno::NOENT) => Ok(()),
                    result => result,
                }
            }
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
        syscall_start!(
            "timerfd_settime",
            self.timer_fd,
            TimerfdTimerFlags::empty(),
            spec
        );
        let result = timerfd_settime(&self.timer_fd, TimerfdTimerFlags::empty(), &spec);
        syscall_end!(result);
        result?;
        Ok(())
    }
}

impl Drop for EpollSelector {
    fn drop(&mut self) {
        syscall!("close", self.epoll);
        syscall!("close", self.timer_fd);
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
