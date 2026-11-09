use core::{cell::Cell, future::Future};

pub(crate) struct DropFlag<'a>(pub(crate) &'a Cell<bool>);

impl Drop for DropFlag<'_> {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

pub(crate) fn flagged_task(dropped: &Cell<bool>) -> impl Future<Output = ()> {
    let flag = DropFlag(dropped);
    async move {
        let _flag = flag;
    }
}
