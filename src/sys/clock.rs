#[cfg(target_os = "macos")]
type ClockId = u32;
#[cfg(target_os = "linux")]
type ClockId = i32;

#[cfg(target_os = "macos")]
const MONOTONIC_CLOCK: ClockId = 8;
#[cfg(target_os = "linux")]
const MONOTONIC_CLOCK: ClockId = 1;

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

// SAFETY: matches `int clock_gettime(clockid_t, struct timespec *)` from libc, where `clockid_t`
// is a C enum (`u32`) on macOS and an `int` (`i32`) on Linux, and `struct timespec` is
// `{ time_t, long }`, both 64-bit on the supported 64-bit targets. Any clock id is accepted (an
// unknown one fails with `EINVAL`), and a `&mut Timespec` is a non-null pointer valid for writes,
// so the call is safe.
#[link(name = "c")]
unsafe extern "C" {
    safe fn clock_gettime(clock_id: ClockId, tp: &mut Timespec) -> i32;
}

pub(crate) fn monotonic_nanos() -> u64 {
    let mut time = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = clock_gettime(MONOTONIC_CLOCK, &mut time);
    assert_eq!(result, 0, "clock_gettime failed");
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}
