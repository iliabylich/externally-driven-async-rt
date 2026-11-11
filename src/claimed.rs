use core::{cell::Cell, marker::PhantomData};

#[derive(Clone, Copy)]
pub(crate) struct Claimed<'a>(PhantomData<(Cell<&'a ()>, *const ())>);

impl Claimed<'_> {
    /// # Safety
    ///
    /// Every task spawned with the returned `Claimed` must be polled and dropped only before `'a`
    /// ends.
    pub(crate) const unsafe fn new() -> Self {
        Self(PhantomData)
    }
}
