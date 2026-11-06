use core::time::Duration;
use std::time::Instant;

use rustix::fd::{AsRawFd, OwnedFd};

use super::kqueue::Selector;
use super::{Direction, IoAction, IoChange, SelectorEvent, Token};
use crate::test_utils::{is_readable, pair};

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

fn collect(selector: &Selector) -> Vec<SelectorEvent> {
    let mut events = Vec::new();
    selector.poll_nonblocking(&mut events).unwrap();
    events
}

#[test]
fn nothing_armed_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (_a, b) = pair();
    selector.flush(&[]).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn armed_write_is_reported_with_its_token() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    selector.flush(&[arm(1, &a, Direction::Write)]).unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Writable(Token(1))]);
}

#[test]
fn read_interest_is_one_shot() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.flush(&[arm(2, &a, Direction::Read)]).unwrap();

    rustix::io::write(&b, b"hello").unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Readable(Token(2))]);
    rustix::io::write(&b, b"more").unwrap();
    assert!(!is_readable(&selector, 0));
    assert_eq!(collect(&selector), vec![]);

    selector.flush(&[arm(2, &a, Direction::Read)]).unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Readable(Token(2))]);
}

#[test]
fn data_that_arrived_before_arming_is_reported() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    rustix::io::write(&b, b"early").unwrap();
    selector.flush(&[arm(3, &a, Direction::Read)]).unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Readable(Token(3))]);
}

#[test]
fn disarming_prevents_events() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.flush(&[arm(4, &a, Direction::Read)]).unwrap();
    selector.flush(&[disarm(4, &a, Direction::Read)]).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
}

#[test]
fn apply_now_deletes_an_armed_filter_without_flush() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.flush(&[arm(5, &a, Direction::Read)]).unwrap();
    selector.apply_now(disarm(5, &a, Direction::Read)).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
}

#[test]
fn deleted_then_closed_fd_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.flush(&[arm(6, &a, Direction::Read)]).unwrap();
    selector.apply_now(disarm(6, &a, Direction::Read)).unwrap();
    drop(a);
    rustix::io::write(&b, b"x").ok();
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn disarming_a_filter_that_already_fired_is_ok() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    selector.flush(&[arm(7, &a, Direction::Write)]).unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Writable(Token(7))]);
    selector.apply_now(disarm(7, &a, Direction::Write)).unwrap();
}

#[test]
fn reused_fd_number_reports_the_new_token() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    selector.flush(&[arm(8, &a, Direction::Write)]).unwrap();
    selector.apply_now(disarm(8, &a, Direction::Write)).unwrap();
    drop(a);

    let (c, _d) = pair();
    selector.flush(&[arm(9, &c, Direction::Write)]).unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Writable(Token(9))]);
}

#[test]
fn many_ready_fds_are_all_delivered() {
    let selector = Selector::new().unwrap();
    let pairs: Vec<_> = (0..100).map(|_| pair()).collect();
    let changes: Vec<_> = pairs
        .iter()
        .enumerate()
        .map(|(i, (a, _))| arm(i as u64, a, Direction::Write))
        .collect();
    selector.arm_timer(Instant::now() + Duration::from_secs(10));
    selector.flush(&changes).unwrap();
    assert_eq!(collect(&selector).len(), pairs.len());
}

#[test]
fn timer_fires_after_flush() {
    let selector = Selector::new().unwrap();
    let start = Instant::now();
    selector.arm_timer(Instant::now() + Duration::from_millis(20));
    selector.flush(&[]).unwrap();
    assert!(is_readable(&selector, 500));
    assert!(start.elapsed() >= Duration::from_millis(20));
    assert_eq!(collect(&selector), vec![SelectorEvent::Timer]);
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
    selector.flush(&[]).unwrap();
    assert!(!is_readable(&selector, 50));

    selector.arm_timer(Instant::now() + Duration::from_millis(10));
    selector.flush(&[]).unwrap();
    assert!(is_readable(&selector, 500));
    assert_eq!(collect(&selector), vec![SelectorEvent::Timer]);
}

#[test]
fn disarm_prevents_firing() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::from_millis(10));
    selector.flush(&[]).unwrap();
    selector.disarm_timer();
    selector.flush(&[]).unwrap();
    assert!(!is_readable(&selector, 50));
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn disarm_after_timer_fired_is_ok() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::ZERO);
    selector.flush(&[]).unwrap();
    assert!(is_readable(&selector, 500));
    selector.disarm_timer();
    selector.flush(&[]).unwrap();
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn disarm_when_never_armed_is_ok() {
    let selector = Selector::new().unwrap();
    selector.disarm_timer();
    selector.flush(&[]).unwrap();
}
