mod io_registration;
mod io_waiter;
mod io_waiters;
mod wait;

use self::io_waiter::IoWaiter;
pub(crate) use self::{
    io_registration::IoRegistration, io_waiter::IoIndex, io_waiters::IoWaiters, wait::Wait,
};

#[cfg(test)]
mod tests;
