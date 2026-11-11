use rustix::fd::{AsFd, BorrowedFd};

use crate::{
    selector::{EVENTS_PER_CALL, IoChange, SelectorEvent},
    time::Instant,
};

#[must_use]
pub(crate) struct DummySelector;

impl DummySelector {
    pub(crate) fn new() -> rustix::io::Result<Self> {
        Ok(Self)
    }

    pub(crate) fn apply_now(&self, _: IoChange) -> rustix::io::Result<()> {
        panic!("no selector under Miri")
    }

    pub(crate) fn arm_timer(&self, _: Instant) {
        panic!("no selector under Miri")
    }

    pub(crate) fn disarm_timer(&self) {}

    pub(crate) fn flush(
        &self,
        io_changes: impl IntoIterator<Item = IoChange>,
    ) -> rustix::io::Result<()> {
        assert!(
            io_changes.into_iter().next().is_none(),
            "no selector under Miri"
        );
        Ok(())
    }

    pub(crate) fn poll_nonblocking(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
    ) -> rustix::io::Result<bool> {
        out.fill(None);
        Ok(false)
    }
}

impl AsFd for DummySelector {
    fn as_fd(&self) -> BorrowedFd<'_> {
        panic!("no selector under Miri")
    }
}
