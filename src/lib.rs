#[cfg(not(target_os = "macos"))]
compile_error!(
    "externally-driven-async-rt currently supports only macOS (Linux support is planned)"
);

extern crate alloc;

mod context;
mod event_loop;
mod inner;
mod io;
mod join_handle;
pub mod net;
#[expect(dead_code)]
mod selectors;
mod sleep;
mod state;
#[expect(dead_code)]
mod task;
#[cfg(test)]
mod test_utils;
mod timers;
mod waker;

pub use context::Context;
pub use event_loop::{Error, EventLoop};
pub use join_handle::JoinHandle;
pub use sleep::Sleep;
pub use state::State;
