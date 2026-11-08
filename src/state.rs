pub trait State: 'static {
    type Event;

    fn apply(&mut self, event: Self::Event);
}
