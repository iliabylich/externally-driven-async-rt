/// An error returned when all `FDS` fd slots are taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FdsFullError;

impl core::fmt::Display for FdsFullError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("all fd slots are taken")
    }
}

impl core::error::Error for FdsFullError {}
