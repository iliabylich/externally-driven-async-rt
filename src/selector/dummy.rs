use core::cell::Cell;

use rustix::fd::{AsFd, BorrowedFd};

use crate::{
    const_unreachable::const_unreachable,
    selector::{EVENTS_PER_CALL, IoChange, SelectorEvent},
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

    pub(crate) fn apply_now(&self, _: IoChange) -> rustix::io::Result<()> {
        const_unreachable!("no selector under Miri")
    }

    pub(crate) fn flush(
        &self,
        io_changes: impl IntoIterator<Item = IoChange>,
        deadline: Option<Instant>,
    ) -> rustix::io::Result<()> {
        assert!(
            io_changes.into_iter().next().is_none() && deadline.is_none(),
            "no selector under Miri"
        );
        Ok(())
    }

    pub(crate) fn poll_nonblocking(
        &self,
        out: &mut [Option<SelectorEvent>; EVENTS_PER_CALL],
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
        const_unreachable!("no selector under Miri")
    }
}
