#![no_std]
#![doc = include_str!("../README.md")]
#![warn(
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_qualifications,
    deprecated_in_future,
    unused_lifetimes
)]
#![allow(clippy::new_without_default)]
#![cfg_attr(test, allow(clippy::arithmetic_side_effects))]

mod context;
mod event_loop;
mod io;
/// A module that implements TCP/UDP/Unix sockets.
pub mod net;
mod pool;
mod selector;
mod state;
mod storage;
mod strace;
mod sys;
mod tasks;
#[cfg(test)]
mod testing;
mod time;
mod waker_slot;

pub use context::Context;
pub use event_loop::{Error, EventLoop};
pub use selector::Selector;
pub use state::State;
pub use storage::LoopStorage;
pub use tasks::{JoinHandle, ReserveTask, TaskReservation};
pub use time::{Sleep, Timeout};
