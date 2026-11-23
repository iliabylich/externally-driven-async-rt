use core::{cell::Cell, time::Duration};

use rustix::{
    event::{
        EventfdFlags, Timespec,
        epoll::{self, CreateFlags, Event, EventData, EventFlags},
        eventfd,
    },
    fd::{AsFd, BorrowedFd, OwnedFd},
    io::Errno,
    time::{
        Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, timerfd_create,
        timerfd_settime,
    },
};

use crate::{
    const_default::ConstDefault,
    const_unreachable::const_unreachable,
    selector::{Directions, EVENTS_PER_CALL, IoChange, KernelTimer, SelectorEvent, TimerChange},
    strace::{syscall, syscall_end, syscall_start},
    time::Instant,
};

const TIMER_DATA: u64 = u64::MAX;
const NOTIFY_DATA: u64 = u64::MAX - 1;
const ZERO: Timespec = Timespec {
    tv_sec: 0,
    tv_nsec: 0,
};
const NO_EVENT: Event = Event {
    flags: EventFlags::empty(),
    data: EventData::new_u64(0),
};

#[must_use]
pub(crate) struct EpollSelector {
    epoll: OwnedFd,
    timer_fd: OwnedFd,
    notify_fd: OwnedFd,
    timer: Cell<KernelTimer>,
}

impl EpollSelector {
    pub(crate) fn new() -> rustix::io::Result<Self> {
        syscall_start!("epoll_create1", CreateFlags::CLOEXEC);
        let epoll = epoll::create(CreateFlags::CLOEXEC);
        syscall_end!(epoll);
        let epoll = epoll?;

        let timer_flags = TimerfdFlags::NONBLOCK | TimerfdFlags::CLOEXEC;
        syscall_start!("timerfd_create", TimerfdClockId::Monotonic, timer_flags);
        let timer_fd = timerfd_create(TimerfdClockId::Monotonic, timer_flags);
        syscall_end!(timer_fd);
        let timer_fd = timer_fd?;

        let notify_flags = EventfdFlags::NONBLOCK | EventfdFlags::CLOEXEC;
        syscall_start!("eventfd2", 0, notify_flags);
        let notify_fd = eventfd(0, notify_flags);
        syscall_end!(notify_fd);
        let notify_fd = notify_fd?;

        add_readable(&epoll, &timer_fd, TIMER_DATA)?;
        add_readable(&epoll, &notify_fd, NOTIFY_DATA)?;

        Ok(Self {
            epoll,
            timer_fd,
            notify_fd,
            timer: Cell::new(KernelTimer::DEFAULT),
        })
    }

    pub(crate) fn notify(&self) -> rustix::io::Result<()> {
        let increment = 1_u64.to_ne_bytes();
        syscall_start!("write", self.notify_fd, &increment[..], increment.len());
        let result = rustix::io::write(&self.notify_fd, &increment);
        syscall_end!(result);
        match result {
            Ok(_) | Err(Errno::AGAIN) => Ok(()),
            Err(errno) => Err(errno),
        }
    }

    pub(crate) fn apply_now(&self, change: IoChange) -> rustix::io::Result<()> {
        self.apply_io(change)
    }

    pub(crate) fn flush(
        &self,
        io_changes: impl IntoIterator<Item = IoChange>,
        deadline: Option<Instant>,
        now: Instant,
    ) -> rustix::io::Result<()> {
        for change in io_changes {
            self.apply_io(change)?;
        }
        let (timer, timer_change) = self.timer.get().flush(deadline, now);
        self.timer.set(timer);
        let Some(change) = timer_change else {
            return Ok(());
        };
        self.apply_timer(change)
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
                Err(Errno::INTR) => {}
                Err(errno) => return Err(errno),
            }
        };
        let mut consumed = [None; EVENTS_PER_CALL];
        for (event, slot) in buf.iter().take(len).zip(&mut consumed) {
            *slot = self.consume(*event)?;
        }
        let mut translated = consumed.into_iter().flatten();
        out.fill_with(|| translated.next());
        Ok(len == EVENTS_PER_CALL)
    }

    fn consume(&self, event: Event) -> rustix::io::Result<Option<SelectorEvent>> {
        match event.data.u64() {
            TIMER_DATA => self.consume_timer(),
            NOTIFY_DATA => self.consume_notify(),
            data => {
                let Ok(fd) = u32::try_from(data) else {
                    const_unreachable!("event data {} is not an fd", data);
                };
                let fd = fd.cast_signed();
                let ready = Self::ready(event.flags);
                Ok((ready != Directions::Empty).then_some(SelectorEvent::Io { fd, ready }))
            }
        }
    }

    fn ready(flags: EventFlags) -> Directions {
        let readable = flags
            .intersects(EventFlags::IN | EventFlags::RDHUP | EventFlags::HUP | EventFlags::ERR);
        let writable = flags.intersects(EventFlags::OUT | EventFlags::HUP | EventFlags::ERR);
        match (readable, writable) {
            (true, true) => Directions::Both,
            (true, false) => Directions::Read,
            (false, true) => Directions::Write,
            (false, false) => Directions::Empty,
        }
    }

    fn consume_timer(&self) -> rustix::io::Result<Option<SelectorEvent>> {
        let mut expirations = [0; 8];
        syscall_start!("read", self.timer_fd);
        let result = rustix::io::read(&self.timer_fd, &mut expirations);
        syscall_end!(result; &expirations[..result.unwrap_or(0)], expirations.len());
        match result {
            Ok(_) => {
                self.timer.set(KernelTimer::Disarmed);
                Ok(Some(SelectorEvent::Timer))
            }
            Err(Errno::AGAIN) => Ok(None),
            Err(errno) => Err(errno),
        }
    }

    fn consume_notify(&self) -> rustix::io::Result<Option<SelectorEvent>> {
        let mut counter = [0; 8];
        syscall_start!("read", self.notify_fd);
        let result = rustix::io::read(&self.notify_fd, &mut counter);
        syscall_end!(result; &counter[..result.unwrap_or(0)], counter.len());
        match result {
            Ok(_) => Ok(Some(SelectorEvent::Notify)),
            Err(Errno::AGAIN) => Ok(None),
            Err(errno) => Err(errno),
        }
    }

    fn apply_io(&self, change: IoChange) -> rustix::io::Result<()> {
        // SAFETY: the callers (`flush`, `apply_now`) only pass fds that stay open for the
        // duration of this call.
        let fd = unsafe { BorrowedFd::borrow_raw(change.fd) };
        let interest = match change.interest {
            Directions::Empty => {
                syscall_start!("epoll_ctl", self.epoll, "EPOLL_CTL_DEL", fd);
                let deleted = epoll::delete(&self.epoll, fd);
                syscall_end!(deleted);
                return match deleted {
                    Err(Errno::NOENT) => Ok(()),
                    result => result,
                };
            }
            Directions::Read => EventFlags::IN,
            Directions::Write => EventFlags::OUT,
            Directions::Both => EventFlags::IN | EventFlags::OUT,
        };
        let data = EventData::new_u64(u64::from(change.fd.cast_unsigned()));
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
        syscall!("close", self.notify_fd);
    }
}

fn add_readable(epoll: &OwnedFd, fd: &OwnedFd, data: u64) -> rustix::io::Result<()> {
    let data = EventData::new_u64(data);
    let flags = EventFlags::IN;
    syscall_start!(
        "epoll_ctl",
        epoll,
        "EPOLL_CTL_ADD",
        fd,
        Event { flags, data }
    );
    let result = epoll::add(epoll, fd, data, flags);
    syscall_end!(result);
    result
}

impl AsFd for EpollSelector {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.epoll.as_fd()
    }
}
