mod async_read;
mod async_read_ext;
mod async_write;
mod async_write_ext;
mod io_registration;
mod io_waiter;
mod io_waiters;
mod wait;

use self::io_waiter::IoWaiter;
pub use self::{
    async_read::AsyncRead,
    async_read_ext::{AsyncReadExt, ReadExactError},
    async_write::AsyncWrite,
    async_write_ext::{AsyncWriteExt, WriteAllError},
};
pub(crate) use self::{
    io_registration::IoRegistration, io_waiter::IoIndex, io_waiters::IoWaiters, wait::Wait,
};

#[cfg(test)]
mod tests;
