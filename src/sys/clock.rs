use rustix::time::{ClockId, clock_gettime};

#[must_use]
pub(crate) fn monotonic_nanos() -> u64 {
    let time = clock_gettime(ClockId::Monotonic);
    time.tv_sec
        .cast_unsigned()
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(time.tv_nsec).unwrap_or_default())
}
