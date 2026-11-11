const _: () = assert!(usize::BITS == 64, "only 64-bit targets are supported");

#[must_use]
pub(crate) const fn u32_to_usize(value: u32) -> usize {
    let [b0, b1, b2, b3] = value.to_le_bytes();
    usize::from_le_bytes([b0, b1, b2, b3, 0, 0, 0, 0])
}
