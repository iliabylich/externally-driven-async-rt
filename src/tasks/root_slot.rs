#[must_use]
pub(crate) struct RootSlot(());

impl RootSlot {
    pub(crate) const fn new() -> Self {
        Self(())
    }
}
