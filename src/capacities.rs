#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacities {
    pub max_tasks: usize,
    pub max_task_size: usize,
    pub max_timers: usize,
    pub max_io: usize,
}
