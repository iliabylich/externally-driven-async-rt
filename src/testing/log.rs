use crate::State;

#[derive(Default)]
pub(crate) struct Log {
    events: [u32; 32],
    len: usize,
}

impl Log {
    pub(crate) fn events(&self) -> &[u32] {
        &self.events[..self.len]
    }

    pub(crate) fn events_mut(&mut self) -> &mut [u32] {
        &mut self.events[..self.len]
    }
}

impl State for Log {
    type Event = u32;

    fn apply(&mut self, event: u32) {
        assert!(self.len < self.events.len(), "the test Log is full");
        self.events[self.len] = event;
        self.len += 1;
    }
}
