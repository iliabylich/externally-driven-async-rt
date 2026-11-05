#[cfg(not(target_os = "macos"))]
compile_error!(
    "externally-driven-async-rt currently supports only macOS (Linux support is planned)"
);

mod backend;
mod context;
mod event_loop;
mod inner;
mod net;
mod task;
mod timers;
mod waker;
