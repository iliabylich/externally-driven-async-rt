mod direction;
mod io_action;
mod io_change;
mod kernel_timer;
#[cfg(target_os = "macos")]
mod kqueue;
mod selector_event;
mod timer_change;
mod token;

#[cfg(target_os = "macos")]
pub type Selector = kqueue::KqueueSelector;
#[cfg(not(target_os = "macos"))]
compile_error!(
    "externally-driven-async-rt currently supports only macOS (Linux support is planned)"
);

pub(crate) use self::{
    direction::Direction,
    io_action::IoAction,
    io_change::IoChange,
    kernel_timer::KernelTimer,
    selector_event::{EVENTS_PER_CALL, SelectorEvent},
    timer_change::TimerChange,
    token::Token,
};

#[cfg(test)]
mod tests;
