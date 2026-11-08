#![no_std]

#[cfg(not(target_os = "macos"))]
compile_error!(
    "externally-driven-async-rt currently supports only macOS (Linux support is planned)"
);

mod context;
mod current_thread_queue;
mod event_loop;
mod instant;
mod io;
mod join_handle;
pub mod net;
mod pool;
mod ready;
mod selectors;
mod sleep;
mod state;
mod storage;
mod task;
#[cfg(test)]
mod test_utils;
mod thread_id;
mod waker;

pub use context::Context;
pub use event_loop::{Error, EventLoop};
pub use join_handle::JoinHandle;
pub use sleep::Sleep;
pub use state::State;
