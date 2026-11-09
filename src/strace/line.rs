use core::fmt::{self, Write};

use rustix::{fd::BorrowedFd, io::Errno};

use crate::strace::{TraceArg, TraceResult};

const CAPACITY: usize = 512;
const GREEN: &str = "\x1b[32m";
const ORANGE: &str = "\x1b[38;5;208m";
const RESET: &str = "\x1b[0m";
const ELLIPSIS: &str = "\x1b[0m...";
const RESERVED: usize = ELLIPSIS.len() + 1;

pub(crate) struct Line {
    buf: [u8; CAPACITY],
    len: usize,
    truncated: bool,
}

impl Line {
    pub(crate) fn start(name: &str, args: &[&dyn TraceArg]) {
        let mut line = Self::new();
        let _ =
            write!(line, "{GREEN}{name}{RESET}{ORANGE}({RESET}").and_then(|()| line.args(args, ""));
        line.emit("");
    }

    pub(crate) fn end<T: TraceResult>(args: &[&dyn TraceArg], result: &Result<T, Errno>) {
        let mut line = Self::new();
        let _ = line
            .args(args, ", ")
            .and_then(|()| write!(line, "{ORANGE}){RESET} = "))
            .and_then(|()| line.result(result));
        line.emit("\n");
    }

    pub(crate) fn call(name: &str, args: &[&dyn TraceArg]) {
        let mut line = Self::new();
        let _ = write!(line, "{GREEN}{name}{RESET}{ORANGE}({RESET}")
            .and_then(|()| line.args(args, ""))
            .and_then(|()| write!(line, "{ORANGE}){RESET}"));
        line.emit("\n");
    }

    fn new() -> Self {
        Self {
            buf: [0; CAPACITY],
            len: 0,
            truncated: false,
        }
    }

    fn args(&mut self, args: &[&dyn TraceArg], first_separator: &str) -> fmt::Result {
        for (i, arg) in args.iter().enumerate() {
            self.write_str(if i == 0 { first_separator } else { ", " })?;
            arg.trace_arg(self)?;
        }
        Ok(())
    }

    fn result<T: TraceResult>(&mut self, result: &Result<T, Errno>) -> fmt::Result {
        match result {
            Ok(value) => value.trace_result(self),
            Err(errno) => {
                self.write_str("-1 ")?;
                errno.trace_arg(self)
            }
        }
    }

    fn emit(mut self, terminator: &str) {
        let tail = if self.truncated { ELLIPSIS } else { "" };
        for part in [tail, terminator] {
            self.buf[self.len..][..part.len()].copy_from_slice(part.as_bytes());
            self.len += part.len();
        }
        // SAFETY: fd 2 is the process's stderr, which stays open for the whole process lifetime,
        // the same assumption `rustix::stdio::stderr` documents.
        let stderr = unsafe { BorrowedFd::borrow_raw(2) };
        let _ = rustix::io::write(stderr, &self.buf[..self.len]);
    }
}

impl Write for Line {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let room = CAPACITY - RESERVED - self.len;
        if s.len() > room {
            self.buf[self.len..][..room].copy_from_slice(&s.as_bytes()[..room]);
            self.len += room;
            self.truncated = true;
            return Err(fmt::Error);
        }
        self.buf[self.len..][..s.len()].copy_from_slice(s.as_bytes());
        self.len += s.len();
        Ok(())
    }
}
