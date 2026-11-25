use core::future::Future;

/// A stream that can be read from asynchronously.
pub trait AsyncRead {
    /// The error returned by [`read`](Self::read).
    type Error;

    /// Reads into a given buffer, returns the number of read bytes.
    ///
    /// Returns `0` if the stream is closed (or if the buffer is empty).
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    ///
    /// # Cancel safety
    ///
    /// Implementations in this crate are cancel safe.
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>>;
}
