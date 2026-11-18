#[derive(Clone, Copy)]
pub(crate) enum Slot {
    Vacant {
        generation: u32,
        next_free: Option<u32>,
    },
    Occupied {
        generation: u32,
        prev: Option<u32>,
        next: Option<u32>,
    },
}
