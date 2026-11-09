#[cfg(feature = "strace-like")]
mod line;
#[cfg(feature = "strace-like")]
mod trace_arg;
#[cfg(feature = "strace-like")]
mod trace_result;

#[cfg(feature = "strace-like")]
pub(crate) use self::{line::Line, trace_arg::TraceArg, trace_result::TraceResult};

macro_rules! syscall_start {
    ($name:literal $(, $arg:expr)* $(,)?) => {
        #[cfg(feature = "strace-like")]
        $crate::strace::Line::start($name, &[$(&$arg as &dyn $crate::strace::TraceArg),*]);
    };
}

macro_rules! syscall_end {
    ($result:expr $(; $($arg:expr),+ $(,)?)?) => {
        #[cfg(feature = "strace-like")]
        $crate::strace::Line::end(
            &[$($(&$arg as &dyn $crate::strace::TraceArg),+)?],
            &$result,
        );
    };
}

macro_rules! syscall {
    ($name:literal $(, $arg:expr)* $(,)?) => {
        #[cfg(feature = "strace-like")]
        $crate::strace::Line::call($name, &[$(&$arg as &dyn $crate::strace::TraceArg),*]);
    };
}

pub(crate) use {syscall, syscall_end, syscall_start};
