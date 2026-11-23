mod async_read;
mod async_read_ext;
mod async_write;
mod async_write_ext;
mod frame_reader;
mod frame_writer;
mod io_waiter;
mod io_waiters;

use self::io_waiter::IoWaiter;
pub(crate) use self::io_waiters::IoWaiters;
pub use self::{
    async_read::AsyncRead,
    async_read_ext::{AsyncReadExt, ReadExactError},
    async_write::AsyncWrite,
    async_write_ext::{AsyncWriteExt, WriteAllError},
    frame_reader::{FrameReader, FrameReaderError},
    frame_writer::{FrameWriter, FrameWriterError},
};

#[cfg(test)]
mod tests;
