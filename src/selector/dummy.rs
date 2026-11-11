use rustix::fd::{AsFd, BorrowedFd};

use crate::{
    selector::{EVENTS_PER_CALL, IoChange, SelectorEvent},
    time::Instant,
};

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

    pub(crate) fn disarm_timer(&self) {
        panic!("no selector under Miri")
    }

    pub(crate) fn flush(&self, _: impl IntoIterator<Item = IoChange>) -> rustix::io::Result<()> {
        panic!("no selector under Miri")
    }

    pub(crate) fn poll_nonblocking(
        &self,
        _: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
    ) -> rustix::io::Result<bool> {
        panic!("no selector under Miri")
    }
}

impl AsFd for DummySelector {
    fn as_fd(&self) -> BorrowedFd<'_> {
        panic!("no selector under Miri")
    }
}
