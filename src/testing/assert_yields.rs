macro_rules! assert_yields {
    ($actual:expr, $expected:expr $(,)?) => {{
        let mut actual = ::core::iter::IntoIterator::into_iter($actual);
        let mut expected = ::core::iter::IntoIterator::into_iter($expected);
        for i in 0usize.. {
            match (actual.next(), expected.next()) {
                (None, None) => break,
                (actual, expected) => {
                    assert_eq!(actual, expected, "item {i} of `{}`", stringify!($actual))
                }
            }
        }
    }};
}

pub(crate) use assert_yields;
