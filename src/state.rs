pub trait State: 'static {
    type Event: 'static;

    fn apply(&mut self, event: Self::Event);
}
