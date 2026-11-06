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
#[cfg_attr(not(test), expect(dead_code))]
mod selectors;
mod state;
#[expect(dead_code)]
mod task;
#[expect(dead_code)]
mod timers;
#[cfg_attr(not(test), expect(dead_code))]
mod waker;

pub use state::State;
