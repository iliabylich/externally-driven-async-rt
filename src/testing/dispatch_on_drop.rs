use crate::{State, testing::TestContext};

pub(crate) struct DispatchOnDrop<'c, 'a, S: State<Event: Copy>>(
    pub(crate) &'c TestContext<'a, S>,
    pub(crate) S::Event,
);

impl<S: State<Event: Copy>> Drop for DispatchOnDrop<'_, '_, S> {
    fn drop(&mut self) {
        self.0.dispatch(self.1);
    }
}
