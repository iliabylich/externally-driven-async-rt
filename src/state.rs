/// A trait that your custom state must implement.
pub trait State: 'static {
    /// An event type.
    type Event;

    /// Your logic of event handling.
    ///
    /// Internally called when you call [`Context::dispatch`](crate::Context::dispatch).
    fn apply(&mut self, event: Self::Event);
}

impl State for () {
    type Event = ();

    fn apply(&mut self, _: Self::Event) {}
}
