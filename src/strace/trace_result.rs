use core::fmt::{self, Write};
#[cfg(target_os = "macos")]
use core::mem::MaybeUninit;

#[cfg(target_os = "macos")]
use rustix::event::kqueue;
#[cfg(target_os = "linux")]
use rustix::time::Itimerspec;
use rustix::{
    fd::{AsRawFd, OwnedFd},
    io::Errno,
    net::SocketAddrAny,
};

pub(crate) trait TraceResult {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result;
}

impl TraceResult for () {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str("0")
    }
}

impl TraceResult for usize {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{self}")
    }
}

impl TraceResult for OwnedFd {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{}", self.as_raw_fd())
    }
}

impl TraceResult for (OwnedFd, Option<SocketAddrAny>) {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        self.0.trace_result(out)
    }
}

impl TraceResult for (usize, usize) {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        self.0.trace_result(out)
    }
}

impl TraceResult for (usize, usize, Option<SocketAddrAny>) {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        self.0.trace_result(out)
    }
}

impl TraceResult for SocketAddrAny {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str("0")
    }
}

impl TraceResult for Option<SocketAddrAny> {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str("0")
    }
}

impl TraceResult for Result<(), Errno> {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str("0")
    }
}

#[cfg(target_os = "linux")]
impl TraceResult for Itimerspec {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        out.write_str("0")
    }
}

#[cfg(target_os = "macos")]
impl TraceResult for (&mut [kqueue::Event], &mut [MaybeUninit<kqueue::Event>]) {
    fn trace_result(&self, out: &mut dyn Write) -> fmt::Result {
        write!(out, "{}", self.0.len())
    }
}
