use crate::testing::TestContext;

pub(crate) struct DispatchOnDrop<'c, 'a, E: Copy + 'static>(
    pub(crate) &'c TestContext<'a, E>,
    pub(crate) E,
);

impl<E: Copy + 'static> Drop for DispatchOnDrop<'_, '_, E> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}
