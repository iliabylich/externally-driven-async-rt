pub(crate) const fn element<T>(items: &[T], index: usize) -> Option<&T> {
    let Some((_, rest)) = items.split_at_checked(index) else {
        return None;
    };
    rest.first()
}
