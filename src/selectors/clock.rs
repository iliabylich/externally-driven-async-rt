use core::mem::MaybeUninit;

const CLOCK_UPTIME_RAW: u32 = 8;

#[repr(C)]
struct Timespec {
    tv_sec: i64,
    tv_nsec: i64,
}

// SAFETY: matches `int clock_gettime(clockid_t, struct timespec *)` from libSystem, where
// `clockid_t` is a C enum (`u32`) and `struct timespec` is `{ time_t, long }`, both 64-bit.
unsafe extern "C" {
    fn clock_gettime(clock_id: u32, tp: *mut Timespec) -> i32;
}

pub(crate) fn monotonic_nanos() -> u64 {
    let mut time = MaybeUninit::<Timespec>::uninit();
    // SAFETY: `time` is valid for writes of a `Timespec`.
    let result = unsafe { clock_gettime(CLOCK_UPTIME_RAW, time.as_mut_ptr()) };
    assert_eq!(result, 0, "clock_gettime(CLOCK_UPTIME_RAW) failed");
    // SAFETY: `clock_gettime` succeeded, so it initialized `time`.
    let time = unsafe { time.assume_init() };
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}
