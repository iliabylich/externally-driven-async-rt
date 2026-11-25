use core::{cell::Cell, marker::PhantomData};

#[derive(Clone, Copy)]
#[must_use]
pub(crate) struct Claimed<'sel>(PhantomData<(Cell<&'sel ()>, *const ())>);

impl Claimed<'_> {
    /// # Safety
    ///
    /// Every task spawned with the returned `Claimed` must be polled and dropped only before `'sel`
    /// ends.
    pub(crate) const unsafe fn new() -> Self {
        Self(PhantomData)
    }
}
