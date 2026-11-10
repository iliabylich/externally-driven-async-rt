pub trait State: 'static {
    type Event;

    fn apply(&mut self, event: Self::Event);
}

impl State for () {
    type Event = ();

    fn apply(&mut self, _: Self::Event) {}
}
