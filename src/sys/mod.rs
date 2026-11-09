mod clock;
mod thread_id;

pub(crate) use self::{clock::monotonic_nanos, thread_id::ThreadId};
