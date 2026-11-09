mod clock;
mod event;
mod io_change;
#[cfg(target_os = "macos")]
mod kqueue;
#[cfg(all(test, target_os = "macos"))]
mod kqueue_tests;
mod timer;
mod token;

#[cfg(target_os = "macos")]
pub type Selector = kqueue::KqueueSelector;
#[cfg(not(target_os = "macos"))]
compile_error!(
    "externally-driven-async-rt currently supports only macOS (Linux support is planned)"
);

pub(crate) use self::{
    clock::monotonic_nanos,
    event::{EVENTS_PER_CALL, SelectorEvent},
    io_change::{Direction, IoAction, IoChange},
    token::Token,
};
