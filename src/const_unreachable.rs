const CAPACITY: usize = 128;

#[must_use]
pub(crate) struct PanicMessage {
    buf: [u8; CAPACITY],
    len: usize,
    template: &'static [u8],
}

impl PanicMessage {
    pub(crate) const fn new(template: &'static str) -> Self {
        Self {
            buf: [0; CAPACITY],
            len: 0,
            template: template.as_bytes(),
        }
    }

    pub(crate) const fn push_arg<const BYTES: usize>(
        &mut self,
        negative: bool,
        magnitude: [u8; BYTES],
    ) {
        self.push_template_until_placeholder();
        self.push_number(negative, Self::widen(magnitude));
    }

    pub(crate) const fn flush(&mut self) {
        while let [byte, tail @ ..] = self.template {
            self.push_byte(*byte);
            self.template = tail;
        }
    }

    pub(crate) const fn as_str(&self) -> &str {
        let Some((filled, _)) = self.buf.split_at_checked(self.len) else {
            return "";
        };
        match core::str::from_utf8(filled) {
            Ok(message) => message,
            Err(_) => "",
        }
    }

    const fn widen<const BYTES: usize>(bytes: [u8; BYTES]) -> u64 {
        const {
            assert!(BYTES <= 8, "only integers up to 64 bits are supported");
        }
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        let mut rest: &[u8] = &bytes;
        while let [byte, tail @ ..] = rest {
            value |= u64::from_le_bytes([*byte, 0, 0, 0, 0, 0, 0, 0]).wrapping_shl(shift);
            shift = shift.wrapping_add(8);
            rest = tail;
        }
        value
    }

    const fn push_template_until_placeholder(&mut self) {
        loop {
            let template = self.template;
            match template {
                [b'{', b'}', tail @ ..] => {
                    self.template = tail;
                    return;
                }
                [byte, tail @ ..] => {
                    self.push_byte(*byte);
                    self.template = tail;
                }
                [] => return,
            }
        }
    }

    const fn push_byte(&mut self, byte: u8) {
        if let Some((_, rest)) = self.buf.split_at_mut_checked(self.len)
            && let Some(slot) = rest.first_mut()
        {
            *slot = byte;
            self.len = self.len.wrapping_add(1);
        }
    }

    const fn push_number(&mut self, negative: bool, magnitude: u64) {
        if negative {
            self.push_byte(b'-');
        }
        let mut digits = [0u8; 20];
        let mut cursor: &mut [u8] = &mut digits;
        let mut rest = magnitude;
        while let [head @ .., last] = cursor {
            let [digit, ..] = rest.wrapping_rem(10).to_le_bytes();
            *last = b'0'.wrapping_add(digit);
            rest = rest.wrapping_div(10);
            if rest == 0 {
                break;
            }
            cursor = head;
        }
        let mut written: &[u8] = &digits;
        while let [byte, tail @ ..] = written {
            if *byte != 0 {
                self.push_byte(*byte);
            }
            written = tail;
        }
    }
}

macro_rules! const_unreachable {
    ($message:literal $(,)?) => {
        panic!(concat!("internal error: entered unreachable code: ", $message))
    };
    ($template:literal, $($arg:expr),+ $(,)?) => {{
        let mut message = $crate::const_unreachable::PanicMessage::new(concat!(
            "internal error: entered unreachable code: ",
            $template
        ));
        $({
            let value = $arg;
            let zero = value.wrapping_sub(value);
            message.push_arg(value < zero, value.abs_diff(zero).to_le_bytes());
        })+
        message.flush();
        panic!("{}", message.as_str())
    }};
}

pub(crate) use const_unreachable;

#[cfg(test)]
mod tests {
    use crate::const_unreachable::PanicMessage;

    #[test]
    fn substitutes_numbers_in_order() {
        let mut message = PanicMessage::new("{} vs {}");
        message.push_arg(false, 7u32.to_le_bytes());
        message.push_arg(false, 0u32.to_le_bytes());
        message.flush();
        assert_eq!(message.as_str(), "7 vs 0");
    }

    #[test]
    fn prints_the_largest_numbers() {
        let mut message = PanicMessage::new("{} {}");
        message.push_arg(false, u32::MAX.to_le_bytes());
        message.push_arg(false, u64::MAX.to_le_bytes());
        message.flush();
        assert_eq!(message.as_str(), "4294967295 18446744073709551615");
    }

    #[test]
    fn prints_negative_numbers() {
        let mut message = PanicMessage::new("{} {}");
        message.push_arg(true, 1u32.to_le_bytes());
        message.push_arg(true, i64::MIN.unsigned_abs().to_le_bytes());
        message.flush();
        assert_eq!(message.as_str(), "-1 -9223372036854775808");
    }

    #[test]
    fn keeps_a_placeholder_without_an_argument() {
        let mut message = PanicMessage::new("{} and {}");
        message.push_arg(false, 1u8.to_le_bytes());
        message.flush();
        assert_eq!(message.as_str(), "1 and {}");
    }

    #[test]
    fn appends_an_argument_without_a_placeholder() {
        let mut message = PanicMessage::new("x");
        message.push_arg(false, 5u8.to_le_bytes());
        message.flush();
        assert_eq!(message.as_str(), "x5");
    }

    #[test]
    fn widens_every_unsigned_type() {
        assert_eq!(PanicMessage::widen(0xABu8.to_le_bytes()), 0xAB);
        assert_eq!(PanicMessage::widen(0xBEEFu16.to_le_bytes()), 0xBEEF);
        assert_eq!(
            PanicMessage::widen(u32::MAX.to_le_bytes()),
            u64::from(u32::MAX)
        );
        assert_eq!(PanicMessage::widen(u64::MAX.to_le_bytes()), u64::MAX);
    }

    #[test]
    fn stops_at_the_capacity() {
        static LONG: [u8; 200] = [b'x'; 200];
        let mut message = PanicMessage::new(core::str::from_utf8(&LONG).unwrap());
        message.flush();
        assert_eq!(message.as_str().len(), 128);
    }
}
