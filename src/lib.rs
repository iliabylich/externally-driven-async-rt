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
#![cfg_attr(test, allow(clippy::arithmetic_side_effects, clippy::as_conversions))]

mod claimed;
mod context;
mod conversions;
mod event_loop;
/// IO traits, implemented by streams from [`net`].
pub mod io;
/// A module that implements TCP/UDP/Unix sockets.
pub mod net;
mod notify;
mod pool;
mod select;
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
pub use notify::Notified;
pub use select::{Either, select};
pub use selector::Selector;
pub use state::State;
pub use storage::LoopStorage;
pub use tasks::{JoinHandle, ReserveTask, TaskReservation};
pub use time::{Sleep, Timeout};
