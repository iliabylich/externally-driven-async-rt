#[cfg(not(target_os = "macos"))]
compile_error!(
    "externally-driven-async-rt currently supports only macOS (Linux support is planned)"
);

extern crate alloc;

mod context;
mod event_loop;
#[expect(dead_code)]
mod inner;
mod net;
#[expect(dead_code)]
mod selectors;
mod state;
#[expect(dead_code)]
mod task;
#[expect(dead_code)]
mod timers;
mod waker;

pub use context::{Context, JoinHandle};
pub use event_loop::{Error, EventLoop};
pub use state::State;
