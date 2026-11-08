use core::time::Duration;

use rustix::fd::{AsRawFd, OwnedFd};

use super::kqueue::Selector;
use super::{Direction, EVENTS_PER_CALL, IoAction, IoChange, SelectorEvent, Token};
use crate::instant::Instant;
use crate::test_utils::{assert_yields, is_readable, pair};

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

// Every pending event, polling batch after batch while they come back full.
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
fn nothing_armed_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (_a, b) = pair();
    selector.flush([]).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
    assert_yields(events(&selector), []);
}

#[test]
fn armed_write_is_reported_with_its_token() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    selector.flush([arm(1, &a, Direction::Write)]).unwrap();
    assert_yields(events(&selector), [SelectorEvent::Writable(Token(1))]);
}

#[test]
fn read_interest_is_one_shot() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.flush([arm(2, &a, Direction::Read)]).unwrap();

    rustix::io::write(&b, b"hello").unwrap();
    assert_yields(events(&selector), [SelectorEvent::Readable(Token(2))]);
    rustix::io::write(&b, b"more").unwrap();
    assert!(!is_readable(&selector, 0));
    assert_yields(events(&selector), []);

    selector.flush([arm(2, &a, Direction::Read)]).unwrap();
    assert_yields(events(&selector), [SelectorEvent::Readable(Token(2))]);
}

#[test]
fn data_that_arrived_before_arming_is_reported() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    rustix::io::write(&b, b"early").unwrap();
    selector.flush([arm(3, &a, Direction::Read)]).unwrap();
    assert_yields(events(&selector), [SelectorEvent::Readable(Token(3))]);
}

#[test]
fn disarming_prevents_events() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.flush([arm(4, &a, Direction::Read)]).unwrap();
    selector.flush([disarm(4, &a, Direction::Read)]).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
}

#[test]
fn apply_now_deletes_an_armed_filter_without_flush() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.flush([arm(5, &a, Direction::Read)]).unwrap();
    selector.apply_now(disarm(5, &a, Direction::Read)).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
}

#[test]
fn deleted_then_closed_fd_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.flush([arm(6, &a, Direction::Read)]).unwrap();
    selector.apply_now(disarm(6, &a, Direction::Read)).unwrap();
    drop(a);
    rustix::io::write(&b, b"x").ok();
    assert_yields(events(&selector), []);
}

#[test]
fn disarming_a_filter_that_already_fired_is_ok() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    selector.flush([arm(7, &a, Direction::Write)]).unwrap();
    assert_yields(events(&selector), [SelectorEvent::Writable(Token(7))]);
    selector.apply_now(disarm(7, &a, Direction::Write)).unwrap();
}

#[test]
fn reused_fd_number_reports_the_new_token() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    selector.flush([arm(8, &a, Direction::Write)]).unwrap();
    selector.apply_now(disarm(8, &a, Direction::Write)).unwrap();
    drop(a);

    let (c, _d) = pair();
    selector.flush([arm(9, &c, Direction::Write)]).unwrap();
    assert_yields(events(&selector), [SelectorEvent::Writable(Token(9))]);
}

#[test]
fn changes_and_events_beyond_one_batch_are_all_delivered() {
    // Changelists and event batches hold 4 entries in tests; both ends of a socket pair are
    // writable, so 3 pairs give 6 changes.
    let selector = Selector::new().unwrap();
    let [(a0, b0), (a1, b1), (a2, b2)] = core::array::from_fn(|_| pair());
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
fn timer_fires_after_flush() {
    let selector = Selector::new().unwrap();
    let start = Instant::now();
    selector.arm_timer(start + Duration::from_millis(20));
    selector.flush([]).unwrap();
    assert!(is_readable(&selector, 500));
    assert!(Instant::now().saturating_duration_since(start) >= Duration::from_millis(20));
    assert_yields(events(&selector), [SelectorEvent::Timer]);
    assert!(!is_readable(&selector, 50));
}

#[test]
fn timer_is_not_armed_without_flush() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::from_millis(1));
    assert!(!is_readable(&selector, 50));
}

#[test]
fn last_timer_change_before_flush_wins() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::from_millis(1));
    selector.arm_timer(Instant::now() + Duration::from_secs(10));
    selector.flush([]).unwrap();
    assert!(!is_readable(&selector, 50));

    selector.arm_timer(Instant::now() + Duration::from_millis(10));
    selector.flush([]).unwrap();
    assert!(is_readable(&selector, 500));
    assert_yields(events(&selector), [SelectorEvent::Timer]);
}

#[test]
fn disarm_prevents_firing() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::from_millis(10));
    selector.flush([]).unwrap();
    selector.disarm_timer();
    selector.flush([]).unwrap();
    assert!(!is_readable(&selector, 50));
    assert_yields(events(&selector), []);
}

#[test]
fn disarm_after_timer_fired_is_ok() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::ZERO);
    selector.flush([]).unwrap();
    assert!(is_readable(&selector, 500));
    selector.disarm_timer();
    selector.flush([]).unwrap();
    assert_yields(events(&selector), []);
}

#[test]
fn disarm_when_never_armed_is_ok() {
    let selector = Selector::new().unwrap();
    selector.disarm_timer();
    selector.flush([]).unwrap();
}
