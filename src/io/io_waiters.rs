use rustix::fd::{AsRawFd, BorrowedFd, RawFd};

use super::IoWaiter;

use crate::{
    NotEnoughResources,
    data::fd_hash_map::FdHashMap,
    net::NetError,
    selector::{Directions, Selector},
};

#[must_use]
pub(crate) struct IoWaiters<const N: usize> {
    map: FdHashMap<IoWaiter, N>,
}

impl<const N: usize> IoWaiters<N> {
    pub(crate) const fn new() -> Self {
        Self {
            map: FdHashMap::new([const { IoWaiter::new() }; N]),
        }
    }

    pub(crate) fn register(
        &self,
        selector: &Selector,
        fd: BorrowedFd<'_>,
    ) -> Result<&IoWaiter, NetError> {
        let raw_fd = fd.as_raw_fd();
        let io = self.map.insert(raw_fd).ok_or(NotEnoughResources::FDs)?;
        if let Err(errno) = selector.register(fd) {
            self.map.remove(raw_fd);
            return Err(errno.into());
        }
        io.reset();
        Ok(io)
    }

    pub(crate) fn deregister(&self, selector: &Selector, fd: BorrowedFd<'_>) {
        let raw_fd = fd.as_raw_fd();
        if let Err(errno) = selector.deregister(fd) {
            unreachable!("deregistering fd {raw_fd} failed: {errno}");
        }
        self.map.remove(raw_fd);
    }

    pub(crate) fn wake_ready(&self, fd: RawFd, ready: Directions) {
        if let Some(io) = self.map.get(fd) {
            io.wake(ready);
        }
    }

    pub(crate) fn drop_wakers(&self) {
        for (_, io) in self.map.iter() {
            io.drop_wakers();
        }
    }
}
