/// An error returned from [`Context::spawn`](crate::Context::spawn) when all `TASKS` task slots
/// are taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TasksFullError;

impl core::fmt::Display for TasksFullError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("all task slots are taken")
    }
}

impl core::error::Error for TasksFullError {}
