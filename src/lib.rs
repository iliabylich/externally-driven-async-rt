#![no_std]
#![doc = include_str!("../README.md")]
#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_qualifications,
    deprecated_in_future,
    unused_lifetimes
)]
#![cfg_attr(test, allow(clippy::arithmetic_side_effects, clippy::as_conversions))]

mod claimed;
mod const_default;
mod const_unreachable;
mod context;
mod conversions;
mod data;
mod event_loop;
mod event_queue;
mod fd_hash_map;
mod helpers;
/// IO traits, implemented by streams from [`net`].
pub mod io;
/// A module that implements TCP/UDP/Unix sockets.
pub mod net;
mod notify;
mod select;
mod selector;
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
pub use select::{Either2, Either3, select2, select3};
pub use selector::Selector;
pub use storage::LoopStorage;
pub use tasks::{JoinHandle, ReserveTask, SlotWaiterTakenError, TaskReservation, TasksFullError};
pub use time::{Sleep, TimersFullError};
