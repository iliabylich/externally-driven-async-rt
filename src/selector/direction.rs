#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Directions {
    Empty,
    Read,
    Write,
    Both,
}

impl Directions {
    pub(crate) fn with(self, direction: Direction) -> Self {
        match (self, direction) {
            (Self::Empty | Self::Read, Direction::Read) => Self::Read,
            (Self::Empty | Self::Write, Direction::Write) => Self::Write,
            _ => Self::Both,
        }
    }

    pub(crate) fn without(self, direction: Direction) -> Self {
        match (self, direction) {
            (Self::Both, Direction::Read) => Self::Write,
            (Self::Both, Direction::Write) => Self::Read,
            (Self::Read, Direction::Read) | (Self::Write, Direction::Write) => Self::Empty,
            (directions, _) => directions,
        }
    }

    pub(crate) fn contains(self, direction: Direction) -> bool {
        matches!(
            (self, direction),
            (Self::Both, _) | (Self::Read, Direction::Read) | (Self::Write, Direction::Write)
        )
    }
}

impl From<Direction> for Directions {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::Read => Self::Read,
            Direction::Write => Self::Write,
        }
    }
}
