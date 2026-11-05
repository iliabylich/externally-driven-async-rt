#[cfg(not(target_os = "macos"))]
compile_error!(
    "externally-driven-async-rt currently supports only macOS (Linux support is planned)"
);

extern crate alloc;

mod context;
mod event_loop;
mod inner;
mod net;
#[expect(dead_code, unused_imports)]
mod selectors;
mod task;
#[expect(dead_code)]
mod timers;
mod waker;
