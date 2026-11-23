use crate::claimed::Claimed;

pub(crate) fn claimed_forever() -> Claimed<'static> {
    // SAFETY: `'static` never ends.
    unsafe { Claimed::new() }
}
