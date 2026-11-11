use core::future::Future;

use rustix::io::Errno;

use crate::{Context, State, io::AsyncWrite};

/// Helper methods on top of [`AsyncWrite`], implemented for every [`AsyncWrite`].
pub trait AsyncWriteExt: AsyncWrite {
    /// Writes the whole given buffer, calling [`AsyncWrite::write`] until it's done.
    ///
    /// # Errors
    ///
    /// Returns an error if a syscall fails or if the stream accepts zero bytes.
    fn write_all<
        'a,
        S: State,
        const TASKS: usize,
        const TASK_SIZE: usize,
        const ROOT_TASK_SIZE: usize,
        const TIMERS: usize,
        const FDS: usize,
    >(
        &'a mut self,
        ctx: &'a Context<'_, S, TASKS, TASK_SIZE, ROOT_TASK_SIZE, TIMERS, FDS>,
        mut buf: &'a [u8],
    ) -> impl Future<Output = Result<(), WriteAllError>> {
        async move {
            while !buf.is_empty() {
                let len = match self.write(ctx, buf).await {
                    Ok(0) => return Err(WriteAllError::WriteZero),
                    Ok(len) => len,
                    Err(errno) => return Err(WriteAllError::Io(errno)),
                };
                let Some(rest) = buf.get(len..) else {
                    unreachable!("write() reported {len} written bytes out of {}", buf.len());
                };
                buf = rest;
            }
            Ok(())
        }
    }
}

impl<T: AsyncWrite + ?Sized> AsyncWriteExt for T {}

/// An error returned from [`AsyncWriteExt::write_all`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAllError {
    /// A syscall failed.
    Io(Errno),
    /// The stream accepted zero bytes.
    WriteZero,
}

impl core::fmt::Display for WriteAllError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(errno) => write!(f, "write failed: {errno}"),
            Self::WriteZero => f.write_str("stream accepted zero bytes"),
        }
    }
}

impl core::error::Error for WriteAllError {}

#[cfg(test)]
mod tests {
    use core::{
        cell::RefCell,
        future::{Future, pending, ready},
        net::SocketAddr,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use rustix::{
        io::Errno,
        net::{AddressFamily, SocketType},
    };

    use crate::{
        Selector, State,
        io::{AsyncWrite, AsyncWriteExt, WriteAllError},
        net::TcpStream,
        testing::{EventLoopTestingExt, Log, TestContext, TestLoop, TestStorage, raw_accept},
    };

    struct Scripted {
        max_per_call: usize,
        error: Option<Errno>,
        written: [u8; 32],
        len: usize,
        calls: usize,
    }

    impl Scripted {
        const fn new(max_per_call: usize, error: Option<Errno>) -> Self {
            Self {
                max_per_call,
                error,
                written: [0; 32],
                len: 0,
                calls: 0,
            }
        }

        fn written(&self) -> &[u8] {
            &self.written[..self.len]
        }
    }

    impl AsyncWrite for Scripted {
        fn write<
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
            buf: &'a [u8],
        ) -> impl Future<Output = rustix::io::Result<usize>> {
            self.calls += 1;
            if let Some(errno) = self.error {
                return ready(Err(errno));
            }
            let len = buf.len().min(self.max_per_call);
            self.written[self.len..self.len + len].copy_from_slice(&buf[..len]);
            self.len += len;
            ready(Ok(len))
        }
    }

    fn write_all(
        storage: &'static TestStorage,
        writer: &mut Scripted,
        buf: &[u8],
    ) -> Poll<Result<(), WriteAllError>> {
        let selector = Selector::new().unwrap();
        let state = RefCell::new(Log::default());
        let ctx: TestContext<'_> = storage.claim(&selector, &state);
        let mut cx = Context::from_waker(Waker::noop());
        pin!(writer.write_all(&ctx, buf)).poll(&mut cx)
    }

    #[test]
    fn write_all_retries_partial_writes() {
        let mut writer = Scripted::new(3, None);
        assert_eq!(
            write_all(
                {
                    static STORAGE: TestStorage = TestStorage::new();
                    &STORAGE
                },
                &mut writer,
                b"hello world"
            ),
            Poll::Ready(Ok(()))
        );
        assert_eq!(writer.written(), b"hello world");
        assert_eq!(writer.calls, 4);
    }

    #[test]
    fn write_all_of_nothing_never_writes() {
        let mut writer = Scripted::new(3, None);
        assert_eq!(
            write_all(
                {
                    static STORAGE: TestStorage = TestStorage::new();
                    &STORAGE
                },
                &mut writer,
                b""
            ),
            Poll::Ready(Ok(()))
        );
        assert_eq!(writer.calls, 0);
    }

    #[test]
    fn write_all_fails_when_zero_bytes_are_accepted() {
        let mut writer = Scripted::new(0, None);
        assert_eq!(
            write_all(
                {
                    static STORAGE: TestStorage = TestStorage::new();
                    &STORAGE
                },
                &mut writer,
                b"hello"
            ),
            Poll::Ready(Err(WriteAllError::WriteZero))
        );
        assert_eq!(writer.calls, 1);
    }

    #[test]
    fn write_all_returns_syscall_errors() {
        let mut writer = Scripted::new(3, Some(Errno::PIPE));
        assert_eq!(
            write_all(
                {
                    static STORAGE: TestStorage = TestStorage::new();
                    &STORAGE
                },
                &mut writer,
                b"hello"
            ),
            Poll::Ready(Err(WriteAllError::Io(Errno::PIPE)))
        );
        assert_eq!(writer.calls, 1);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn write_all_fills_a_socket_beyond_its_buffer() {
        const LEN: usize = 1 << 20;
        static DATA: [u8; LEN] = {
            let mut data = [0; LEN];
            let mut i = 0;
            let mut byte = 0;
            while i < LEN {
                data[i] = byte;
                byte = if byte == 250 { 0 } else { byte + 1 };
                i += 1;
            }
            data
        };

        let listener = crate::testing::raw_socket(AddressFamily::INET, SocketType::STREAM);
        let localhost = SocketAddr::from(([127, 0, 0, 1], 0));
        rustix::net::bind(&listener, &localhost).unwrap();
        rustix::net::listen(&listener, 1).unwrap();
        let addr: SocketAddr = rustix::net::getsockname(&listener)
            .unwrap()
            .try_into()
            .unwrap();

        let mut selector = Selector::new().unwrap();
        let mut state = RefCell::new(Log::default());
        let mut event_loop = TestLoop::new(
            {
                static STORAGE: TestStorage = TestStorage::new();
                &STORAGE
            },
            &mut selector,
            &mut state,
            async move |ctx| {
                let mut stream = TcpStream::connect(&ctx, addr).await.unwrap();
                stream.write_all(&ctx, &DATA).await.unwrap();
                ctx.dispatch(1);
                pending::<()>().await;
            },
        )
        .unwrap();

        let peer = raw_accept(&listener);
        let mut received = [0; 4096];
        for chunk in DATA.chunks(received.len()) {
            let received = &mut received[..chunk.len()];
            event_loop.peer_read_exact(&peer, received);
            assert_eq!(received, chunk);
        }
        event_loop.run_until_events(1);
        assert_eq!(event_loop.state_mut().events(), [1]);
    }
}
