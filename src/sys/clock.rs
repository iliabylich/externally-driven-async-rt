const CLOCK_UPTIME_RAW: u32 = 8;

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

// SAFETY: matches `int clock_gettime(clockid_t, struct timespec *)` from libSystem, where
// `clockid_t` is a C enum (`u32`) and `struct timespec` is `{ time_t, long }`, both 64-bit. Any
// clock id is accepted (an unknown one fails with `EINVAL`), and a `&mut Timespec` is a non-null
// pointer valid for writes, so the call is safe.
unsafe extern "C" {
    safe fn clock_gettime(clock_id: u32, tp: &mut Timespec) -> i32;
}

pub(crate) fn monotonic_nanos() -> u64 {
    let mut time = Timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = clock_gettime(CLOCK_UPTIME_RAW, &mut time);
    assert_eq!(result, 0, "clock_gettime(CLOCK_UPTIME_RAW) failed");
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}
