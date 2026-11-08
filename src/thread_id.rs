// The calling thread's `pthread_t`. Unlike `std::thread::ThreadId`, it may be reused once its
// thread has exited; that's fine for the waker check, since a loop's thread outlives its loop
// (`EventLoop` is `!Send`) and wakers must not outlive their loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThreadId(usize);

// SAFETY: matches `pthread_t pthread_self(void)` from libc, where `pthread_t` is a pointer on
// macOS (an `unsigned long` on Linux), both `usize`-sized; it has no preconditions.
unsafe extern "C" {
    safe fn pthread_self() -> usize;
}

impl ThreadId {
    pub(crate) fn current() -> Self {
        Self(pthread_self())
    }
}
