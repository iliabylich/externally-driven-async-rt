use core::future::Future;

/// A stream that can be written to asynchronously.
pub trait AsyncWrite {
    /// The error returned by [`write`](Self::write) and [`flush`](Self::flush).
    type Error;

    /// Writes from a given buffer, returns the number of written bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails.
    ///
    /// # Cancel safety
    ///
    /// Implementations in this crate are cancel safe.
    fn write<'a>(&'a mut self, buf: &'a [u8]) -> impl Future<Output = Result<usize, Self::Error>>;

    /// Makes sure that everything accepted by [`write`](Self::write) is sent.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails.
    ///
    /// # Cancel safety
    ///
    /// Implementations in this crate are cancel safe.
    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>>;
}
