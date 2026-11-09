use core::time::Duration;

use rustix::fd::{AsRawFd, OwnedFd};

use crate::{
    selector::{Direction, EVENTS_PER_CALL, IoAction, IoChange, Selector, SelectorEvent, Token},
    testing::{assert_yields, is_readable, socketpair},
    time::Instant,
};

fn change(token: u64, fd: &OwnedFd, direction: Direction, action: IoAction) -> IoChange {
    IoChange {
        token: Token(token),
        fd: fd.as_raw_fd(),
        direction,
        action,
    }
}

fn arm(token: u64, fd: &OwnedFd, direction: Direction) -> IoChange {
    change(token, fd, direction, IoAction::Arm)
}

fn disarm(token: u64, fd: &OwnedFd, direction: Direction) -> IoChange {
    change(token, fd, direction, IoAction::Disarm)
}

fn events(selector: &Selector) -> impl Iterator<Item = SelectorEvent> + '_ {
    let mut batch = [None; EVENTS_PER_CALL];
    let mut next = EVENTS_PER_CALL;
    let mut full = true;
    core::iter::from_fn(move || {
        loop {
            if let Some(event) = batch.get(next).copied().flatten() {
                next += 1;
                return Some(event);
            }
            if !full {
                return None;
            }
            full = selector.poll_nonblocking(&mut batch).unwrap();
            next = 0;
        }
    })
}

#[test]
#[cfg_attr(miri, ignore)]
fn nothing_armed_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (_rx, tx) = socketpair();
    selector.flush([]).unwrap();
    rustix::io::write(&tx, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
    assert_yields!(events(&selector), []);
}

#[test]
#[cfg_attr(miri, ignore)]
fn armed_write_is_reported_with_its_token() {
    let selector = Selector::new().unwrap();
    let (a, _b) = socketpair();
    selector.flush([arm(1, &a, Direction::Write)]).unwrap();
    assert_yields!(events(&selector), [SelectorEvent::Writable(Token(1))]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn read_interest_is_one_shot() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector.flush([arm(2, &rx, Direction::Read)]).unwrap();

    rustix::io::write(&tx, b"hello").unwrap();
    assert_yields!(events(&selector), [SelectorEvent::Readable(Token(2))]);
    rustix::io::write(&tx, b"more").unwrap();
    assert!(!is_readable(&selector, 0));
    assert_yields!(events(&selector), []);

    selector.flush([arm(2, &rx, Direction::Read)]).unwrap();
    assert_yields!(events(&selector), [SelectorEvent::Readable(Token(2))]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn data_that_arrived_before_arming_is_reported() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    rustix::io::write(&tx, b"early").unwrap();
    selector.flush([arm(3, &rx, Direction::Read)]).unwrap();
    assert_yields!(events(&selector), [SelectorEvent::Readable(Token(3))]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn disarming_prevents_events() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector.flush([arm(4, &rx, Direction::Read)]).unwrap();
    selector.flush([disarm(4, &rx, Direction::Read)]).unwrap();
    rustix::io::write(&tx, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
}

#[test]
#[cfg_attr(miri, ignore)]
fn apply_now_deletes_an_armed_filter_without_flush() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector.flush([arm(5, &rx, Direction::Read)]).unwrap();
    selector.apply_now(disarm(5, &rx, Direction::Read)).unwrap();
    rustix::io::write(&tx, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
}

#[test]
#[cfg_attr(miri, ignore)]
fn deleted_then_closed_fd_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector.flush([arm(6, &rx, Direction::Read)]).unwrap();
    selector.apply_now(disarm(6, &rx, Direction::Read)).unwrap();
    drop(rx);
    rustix::io::write(&tx, b"x").ok();
    assert_yields!(events(&selector), []);
}

#[test]
#[cfg_attr(miri, ignore)]
fn disarming_a_filter_that_already_fired_is_ok() {
    let selector = Selector::new().unwrap();
    let (a, _b) = socketpair();
    selector.flush([arm(7, &a, Direction::Write)]).unwrap();
    assert_yields!(events(&selector), [SelectorEvent::Writable(Token(7))]);
    selector.apply_now(disarm(7, &a, Direction::Write)).unwrap();
}

#[test]
#[cfg_attr(miri, ignore)]
fn reused_fd_number_reports_the_new_token() {
    let selector = Selector::new().unwrap();
    let (a, _b) = socketpair();
    selector.flush([arm(8, &a, Direction::Write)]).unwrap();
    selector.apply_now(disarm(8, &a, Direction::Write)).unwrap();
    drop(a);

    let (c, _d) = socketpair();
    selector.flush([arm(9, &c, Direction::Write)]).unwrap();
    assert_yields!(events(&selector), [SelectorEvent::Writable(Token(9))]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn changes_and_events_beyond_one_batch_are_all_delivered() {
    let selector = Selector::new().unwrap();
    let [(a0, b0), (a1, b1), (a2, b2)] = core::array::from_fn(|_| socketpair());
    let fds = [&a0, &b0, &a1, &b1, &a2, &b2];
    let arm_first = |count: usize| {
        fds[..count]
            .iter()
            .enumerate()
            .map(|(i, fd)| arm(i as u64, fd, Direction::Write))
    };

    selector.flush(arm_first(4)).unwrap();
    assert_eq!(events(&selector).count(), 4);

    selector.arm_timer(Instant::now() + Duration::from_secs(10));
    selector.flush(arm_first(fds.len())).unwrap();
    assert_eq!(events(&selector).count(), fds.len());
}

#[test]
#[cfg_attr(miri, ignore)]
fn timer_fires_after_flush() {
    let selector = Selector::new().unwrap();
    let start = Instant::now();
    selector.arm_timer(start + Duration::from_millis(20));
    selector.flush([]).unwrap();
    assert!(is_readable(&selector, 500));
    assert!(Instant::now().saturating_duration_since(start) >= Duration::from_millis(20));
    assert_yields!(events(&selector), [SelectorEvent::Timer]);
    assert!(!is_readable(&selector, 50));
}

#[test]
#[cfg_attr(miri, ignore)]
fn timer_is_not_armed_without_flush() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::from_millis(1));
    assert!(!is_readable(&selector, 50));
}

#[test]
#[cfg_attr(miri, ignore)]
fn last_timer_change_before_flush_wins() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::from_millis(1));
    selector.arm_timer(Instant::now() + Duration::from_secs(10));
    selector.flush([]).unwrap();
    assert!(!is_readable(&selector, 50));

    selector.arm_timer(Instant::now() + Duration::from_millis(10));
    selector.flush([]).unwrap();
    assert!(is_readable(&selector, 500));
    assert_yields!(events(&selector), [SelectorEvent::Timer]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn disarm_prevents_firing() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::from_millis(10));
    selector.flush([]).unwrap();
    selector.disarm_timer();
    selector.flush([]).unwrap();
    assert!(!is_readable(&selector, 50));
    assert_yields!(events(&selector), []);
}

#[test]
#[cfg_attr(miri, ignore)]
fn disarm_after_timer_fired_is_ok() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::ZERO);
    selector.flush([]).unwrap();
    assert!(is_readable(&selector, 500));
    selector.disarm_timer();
    selector.flush([]).unwrap();
    assert_yields!(events(&selector), []);
}

#[test]
#[cfg_attr(miri, ignore)]
fn disarm_when_never_armed_is_ok() {
    let selector = Selector::new().unwrap();
    selector.disarm_timer();
    selector.flush([]).unwrap();
}
