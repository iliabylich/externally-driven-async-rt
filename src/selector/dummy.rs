use core::{cell::Cell, time::Duration};

use rustix::fd::{AsFd, BorrowedFd};

use crate::{
    selector::{EVENTS_PER_CALL, SelectorEvent},
    time::Instant,
};

#[must_use]
pub(crate) struct DummySelector {
    notified: Cell<bool>,
}

impl DummySelector {
    pub(crate) fn new() -> rustix::io::Result<Self> {
        Ok(Self {
            notified: Cell::new(false),
        })
    }

    pub(crate) fn notify(&self) -> rustix::io::Result<()> {
        self.notified.set(true);
        Ok(())
    }

    pub(crate) fn register(&self, _: BorrowedFd<'_>) -> rustix::io::Result<()> {
        unreachable!("no selector under Miri")
    }

    pub(crate) fn deregister(&self, _: BorrowedFd<'_>) -> rustix::io::Result<()> {
        unreachable!("no selector under Miri")
    }

    pub(crate) fn flush(&self, deadline: Option<Instant>, _: Instant) -> rustix::io::Result<()> {
        assert!(deadline.is_none(), "no selector under Miri");
        Ok(())
    }

    pub(crate) fn poll(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
        _: Option<Duration>,
    ) -> rustix::io::Result<bool> {
        out.fill(None);
        if self.notified.replace(false)
            && let Some(first) = out.first_mut()
        {
            *first = Some(SelectorEvent::Notify);
        }
        Ok(false)
    }
}

impl AsFd for DummySelector {
    fn as_fd(&self) -> BorrowedFd<'_> {
        unreachable!("no selector under Miri")
    }
}
