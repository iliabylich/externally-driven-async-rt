/// An error returned from [`ReserveTask`](crate::ReserveTask) when all `TASKS` task slots are
/// taken and another task already waits for a free one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotWaiterTakenError;

impl core::fmt::Display for SlotWaiterTakenError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("another task already waits for a free task slot")
    }
}

impl core::error::Error for SlotWaiterTakenError {}
