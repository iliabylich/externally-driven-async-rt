pub trait State {
    type Event;

    fn apply(&mut self, event: Self::Event);
}
