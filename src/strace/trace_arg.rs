use core::{
    fmt::{self, Write},
    time::Duration,
};

#[cfg(target_os = "macos")]
use rustix::event::kqueue::{self, EventFilter};
#[cfg(target_os = "linux")]
use rustix::{
    event::{EventfdFlags, Timespec, epoll},
    time::{Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags},
};
use rustix::{
    fd::{AsRawFd, BorrowedFd, OwnedFd},
    io::{Errno, FdFlags},
    net::{AddressFamily, RecvFlags, SendFlags, Shutdown, SocketAddrAny, SocketType},
};

const MAX_SHOWN_BYTES: usize = 32;
const ERRNO_NAMES: [(Errno, &str); 30] = [
    (Errno::ACCESS, "EACCES"),
    (Errno::ADDRINUSE, "EADDRINUSE"),
    (Errno::ADDRNOTAVAIL, "EADDRNOTAVAIL"),
    (Errno::AFNOSUPPORT, "EAFNOSUPPORT"),
    (Errno::AGAIN, "EAGAIN"),
    (Errno::ALREADY, "EALREADY"),
    (Errno::BADF, "EBADF"),
    (Errno::CONNABORTED, "ECONNABORTED"),
    (Errno::CONNREFUSED, "ECONNREFUSED"),
    (Errno::CONNRESET, "ECONNRESET"),
    (Errno::DESTADDRREQ, "EDESTADDRREQ"),
    (Errno::EXIST, "EEXIST"),
    (Errno::HOSTUNREACH, "EHOSTUNREACH"),
    (Errno::INPROGRESS, "EINPROGRESS"),
    (Errno::INTR, "EINTR"),
    (Errno::INVAL, "EINVAL"),
    (Errno::ISCONN, "EISCONN"),
    (Errno::MFILE, "EMFILE"),
    (Errno::MSGSIZE, "EMSGSIZE"),
    (Errno::NETUNREACH, "ENETUNREACH"),
    (Errno::NFILE, "ENFILE"),
    (Errno::NOBUFS, "ENOBUFS"),
    (Errno::NOENT, "ENOENT"),
    (Errno::NOMEM, "ENOMEM"),
    (Errno::NOTCONN, "ENOTCONN"),
    (Errno::NOTSOCK, "ENOTSOCK"),
    (Errno::OPNOTSUPP, "EOPNOTSUPP"),
    (Errno::PERM, "EPERM"),
    (Errno::PIPE, "EPIPE"),
    (Errno::TIMEDOUT, "ETIMEDOUT"),
];

pub(crate) trait TraceArg {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result;
}

impl<T: TraceArg + ?Sized> TraceArg for &T {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        (**self).trace_arg(out)
    }
}

impl<T: TraceArg> TraceArg for Option<T> {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        match self {
            Some(value) => value.trace_arg(out),
            None => out.write_str("NULL"),
        }
    }
}

impl TraceArg for str {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str(self)
    }
}

impl TraceArg for [u8] {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_char('"')?;
        for &byte in self.iter().take(MAX_SHOWN_BYTES) {
            match byte {
                b'\n' => out.write_str("\\n")?,
                b'\r' => out.write_str("\\r")?,
                b'\t' => out.write_str("\\t")?,
                b'"' => out.write_str("\\\"")?,
                b'\\' => out.write_str("\\\\")?,
                0x20..=0x7e => out.write_char(char::from(byte))?,
                _ => write!(out, "\\x{byte:02x}")?,
            }
        }
        out.write_char('"')?;
        if self.len() > MAX_SHOWN_BYTES {
            out.write_str("...")?;
        }
        Ok(())
    }
}

impl TraceArg for bool {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "[{}]", u8::from(*self))
    }
}

impl TraceArg for i32 {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{self}")
    }
}

impl TraceArg for u32 {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{self}")
    }
}

impl TraceArg for usize {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{self}")
    }
}

impl TraceArg for Duration {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{self:?}")
    }
}

impl TraceArg for BorrowedFd<'_> {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{}", self.as_raw_fd())
    }
}

impl TraceArg for OwnedFd {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{}", self.as_raw_fd())
    }
}

impl TraceArg for Errno {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        match ERRNO_NAMES.iter().find(|(errno, _)| errno == self) {
            Some((_, name)) => out.write_str(name),
            None => write!(out, "errno {}", self.raw_os_error()),
        }
    }
}

impl TraceArg for Result<(), Errno> {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        match self {
            Ok(()) => out.write_str("[0]"),
            Err(errno) => {
                out.write_char('[')?;
                errno.trace_arg(out)?;
                out.write_char(']')
            }
        }
    }
}

impl TraceArg for SocketAddrAny {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        if self.address_family() != AddressFamily::UNIX {
            return write!(out, "{{{self:?}}}");
        }
        const SUN_PATH_OFFSET: usize = 2;
        // SAFETY: `as_ptr` points to the `addr_len()` initialized bytes of the address.
        let bytes = unsafe {
            core::slice::from_raw_parts(self.as_ptr().cast::<u8>(), self.addr_len() as usize)
        };
        let path = bytes.get(SUN_PATH_OFFSET..).unwrap_or_default();
        let len = path
            .iter()
            .rposition(|&byte| byte != 0)
            .map_or(0, |last| last + 1);
        out.write_str("{AF_UNIX, ")?;
        path[..len].trace_arg(out)?;
        out.write_char('}')
    }
}

impl TraceArg for AddressFamily {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        match *self {
            AddressFamily::INET => out.write_str("AF_INET"),
            AddressFamily::INET6 => out.write_str("AF_INET6"),
            AddressFamily::UNIX => out.write_str("AF_UNIX"),
            family => write!(out, "{}", family.as_raw()),
        }
    }
}

impl TraceArg for SocketType {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        match *self {
            SocketType::STREAM => out.write_str("SOCK_STREAM"),
            SocketType::DGRAM => out.write_str("SOCK_DGRAM"),
            ty => write!(out, "{}", ty.as_raw()),
        }
    }
}

impl TraceArg for Shutdown {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str(match self {
            Shutdown::Read => "SHUT_RD",
            Shutdown::Write => "SHUT_WR",
            Shutdown::Both => "SHUT_RDWR",
        })
    }
}

macro_rules! flags {
    ($($ty:ty),* $(,)?) => {$(
        impl TraceArg for $ty {
            fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
                let mut names = self.iter_names();
                let Some((first, _)) = names.next() else {
                    return out.write_str("0");
                };
                out.write_str(first)?;
                for (name, _) in names {
                    write!(out, "|{name}")?;
                }
                Ok(())
            }
        }
    )*};
}

flags!(FdFlags, RecvFlags, SendFlags);
#[cfg(target_os = "linux")]
flags!(
    epoll::CreateFlags,
    epoll::EventFlags,
    EventfdFlags,
    TimerfdFlags,
    TimerfdTimerFlags
);
#[cfg(target_os = "macos")]
flags!(kqueue::EventFlags);

#[cfg(target_os = "linux")]
impl TraceArg for TimerfdClockId {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        match self {
            TimerfdClockId::Monotonic => out.write_str("CLOCK_MONOTONIC"),
            clock => write!(out, "{clock:?}"),
        }
    }
}

#[cfg(target_os = "linux")]
impl TraceArg for Timespec {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{{tv_sec={}, tv_nsec={}}}", self.tv_sec, self.tv_nsec)
    }
}

#[cfg(target_os = "linux")]
impl TraceArg for Itimerspec {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str("{it_interval=")?;
        self.it_interval.trace_arg(out)?;
        out.write_str(", it_value=")?;
        self.it_value.trace_arg(out)?;
        out.write_char('}')
    }
}

#[cfg(target_os = "linux")]
impl TraceArg for epoll::Event {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        let event = *self;
        let flags = event.flags;
        out.write_str("{events=")?;
        flags.trace_arg(out)?;
        write!(out, ", data={:#x}}}", event.data.u64())
    }
}

#[cfg(target_os = "linux")]
impl TraceArg for [epoll::Event] {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        list(self, out)
    }
}

#[cfg(target_os = "macos")]
impl TraceArg for kqueue::Event {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str("{filter=")?;
        match self.filter() {
            EventFilter::Read(fd) => write!(out, "EVFILT_READ, ident={fd}")?,
            EventFilter::Write(fd) => write!(out, "EVFILT_WRITE, ident={fd}")?,
            EventFilter::Timer { ident, timer } => {
                write!(out, "EVFILT_TIMER, ident={ident}, timer=")?;
                timer.trace_arg(out)?;
            }
            EventFilter::User { ident, .. } => write!(out, "EVFILT_USER, ident={ident}")?,
            _ => out.write_str("?")?,
        }
        out.write_str(", flags=")?;
        self.flags().trace_arg(out)?;
        write!(out, ", data={}, udata={:p}}}", self.data(), self.udata())
    }
}

#[cfg(target_os = "macos")]
impl TraceArg for [kqueue::Event] {
    fn trace_arg(&self, out: &mut dyn Write) -> fmt::Result {
        list(self, out)
    }
}

fn list<T: TraceArg>(items: &[T], out: &mut dyn Write) -> fmt::Result {
    out.write_char('[')?;
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.write_str(", ")?;
        }
        item.trace_arg(out)?;
    }
    out.write_char(']')
}
