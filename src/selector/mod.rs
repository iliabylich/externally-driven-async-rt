mod direction;
#[cfg(target_os = "linux")]
mod epoll;
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
#[cfg(target_os = "linux")]
pub type Selector = epoll::EpollSelector;
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
compile_error!("externally-driven-async-rt supports only macOS and Linux");

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
