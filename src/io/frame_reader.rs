use crate::{Context, State, io::AsyncRead};

/// Reads frames of exactly `N` bytes from an [`AsyncRead`].
///
/// A partially read frame is kept inside, so unlike [`AsyncReadExt::read_exact`](crate::io::AsyncReadExt::read_exact)
/// reading a frame can be cancelled and resumed.
#[must_use]
pub struct FrameReader<R, const N: usize> {
    inner: R,
    buf: [u8; N],
    filled: usize,
}

impl<R: AsyncRead, const N: usize> FrameReader<R, N> {
    /// Wraps a given reader.
    pub const fn new(inner: R) -> Self {
        Self {
            inner,
            buf: [0; N],
            filled: 0,
        }
    }

    /// Reads the next frame.
    ///
    /// Returns `None` if the stream is closed before the first byte of the frame.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///
    /// 1. reading fails
    /// 2. or if the stream is closed in the middle of the frame.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn read_exact<
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &mut self,
        ctx: &Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
    ) -> Result<Option<[u8; N]>, FrameReaderError<R::Error>> {
        while self.filled < N {
            let Some(rest) = self.buf.get_mut(self.filled..) else {
                unreachable!("filled is below N");
            };
            let len = match self.inner.read(ctx, rest).await {
                Ok(0) if self.filled == 0 => return Ok(None),
                Ok(0) => return Err(FrameReaderError::UnexpectedEof),
                Ok(len) => len,
                Err(err) => return Err(FrameReaderError::Io(err)),
            };
            let Some(filled) = self.filled.checked_add(len).filter(|&filled| filled <= N) else {
                unreachable!("read() reported more read bytes than the buffer has");
            };
            self.filled = filled;
        }
        self.filled = 0;
        Ok(Some(self.buf))
    }
}

impl<R, const N: usize> FrameReader<R, N> {
    /// Returns a mutable reference to the wrapped reader.
    pub const fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }
}

impl<R, const N: usize> core::fmt::Debug for FrameReader<R, N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("FrameReader")
    }
}

/// An error returned from [`FrameReader::read_exact`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameReaderError<E> {
    /// Reading failed.
    Io(E),
    /// The stream was closed in the middle of the frame.
    UnexpectedEof,
}

impl<E: core::fmt::Display> core::fmt::Display for FrameReaderError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "read failed: {err}"),
            Self::UnexpectedEof => f.write_str("stream closed in the middle of the frame"),
        }
    }
}

impl<E: core::error::Error> core::error::Error for FrameReaderError<E> {}

#[cfg(test)]
mod tests {
    use core::{
        cell::RefCell,
        future::{Future, poll_fn},
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use rustix::io::Errno;

    use crate::{
        Selector, State,
        io::{AsyncRead, FrameReader, FrameReaderError},
        testing::{Log, TestContext, TestStorage},
    };

    struct Chunks {
        chunks: &'static [&'static [u8]],
        end: Option<rustix::io::Result<usize>>,
    }

    impl AsyncRead for Chunks {
        type Error = Errno;

        fn read<
            'a,
            S: State,
            const TASKS: usize,
            const TASK_SIZE: usize,
            const ROOT_TASK_SIZE: usize,
            const TIMERS: usize,
            const FDS: usize,
        >(
            &'a mut self,
            _ctx: &'a crate::Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
            buf: &'a mut [u8],
        ) -> impl Future<Output = rustix::io::Result<usize>> {
            poll_fn(move |_| {
                let Some((chunk, rest)) = self.chunks.split_first() else {
                    return self.end.map_or(Poll::Pending, Poll::Ready);
                };
                let len = buf.len().min(chunk.len());
                buf[..len].copy_from_slice(&chunk[..len]);
                self.chunks = rest;
                Poll::Ready(Ok(len))
            })
        }
    }

    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn reads_consecutive_frames_from_partial_reads() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut reader = FrameReader::<_, 5>::new(Chunks {
            chunks: &[b"hel", b"lo", b"wor", b"ld"],
            end: None,
        });

        assert_eq!(
            poll_once(reader.read_exact(&ctx)),
            Poll::Ready(Ok(Some(*b"hello")))
        );
        assert_eq!(
            poll_once(reader.read_exact(&ctx)),
            Poll::Ready(Ok(Some(*b"world")))
        );
    }

    #[test]
    fn resumes_a_cancelled_read() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut reader = FrameReader::<_, 5>::new(Chunks {
            chunks: &[b"he", b"l"],
            end: None,
        });

        assert_eq!(poll_once(reader.read_exact(&ctx)), Poll::Pending);

        reader.inner.chunks = &[b"lo"];
        assert_eq!(
            poll_once(reader.read_exact(&ctx)),
            Poll::Ready(Ok(Some(*b"hello")))
        );
    }

    #[test]
    fn returns_none_when_the_stream_ends_between_frames() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut reader = FrameReader::<_, 5>::new(Chunks {
            chunks: &[b"hello"],
            end: Some(Ok(0)),
        });

        assert_eq!(
            poll_once(reader.read_exact(&ctx)),
            Poll::Ready(Ok(Some(*b"hello")))
        );
        assert_eq!(poll_once(reader.read_exact(&ctx)), Poll::Ready(Ok(None)));
    }

    #[test]
    fn fails_when_the_stream_ends_in_the_middle_of_a_frame() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut reader = FrameReader::<_, 5>::new(Chunks {
            chunks: &[b"hel"],
            end: Some(Ok(0)),
        });

        assert_eq!(
            poll_once(reader.read_exact(&ctx)),
            Poll::Ready(Err(FrameReaderError::UnexpectedEof))
        );
    }

    #[test]
    fn returns_syscall_errors() {
        static STORAGE: TestStorage = TestStorage::new();
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = STORAGE.claim(&selector, &state);
        let mut reader = FrameReader::<_, 5>::new(Chunks {
            chunks: &[],
            end: Some(Err(Errno::CONNRESET)),
        });

        assert_eq!(
            poll_once(reader.read_exact(&ctx)),
            Poll::Ready(Err(FrameReaderError::Io(Errno::CONNRESET)))
        );
    }
}
