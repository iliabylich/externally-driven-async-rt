#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) struct ThreadId(usize);

// SAFETY: matches `pthread_t pthread_self(void)` from libc, where `pthread_t` is a pointer on
// macOS (an `unsigned long` on Linux), both `usize`-sized; it has no preconditions.
#[link(name = "c")]
unsafe extern "C" {
    safe fn pthread_self() -> usize;
}

impl ThreadId {
    pub(crate) fn current() -> Self {
        Self(pthread_self())
    }

    #[must_use]
    pub(crate) const fn to_raw(self) -> usize {
        self.0
    }
}
