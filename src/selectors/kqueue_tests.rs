use core::time::Duration;
use std::time::Instant;

use rustix::fd::AsFd;

use super::kqueue::Selector;
use super::{Interest, SelectorEvent};
use crate::test_utils::{is_readable, pair};

const READ: Interest = Interest {
    read: true,
    write: false,
};
const WRITE: Interest = Interest {
    read: false,
    write: true,
};
const NONE: Interest = Interest {
    read: false,
    write: false,
};

fn collect(selector: &Selector) -> Vec<SelectorEvent> {
    let mut events = Vec::new();
    selector.poll_nonblocking(&mut events).unwrap();
    events
}

#[test]
fn registration_alone_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    selector.register(a.as_fd());
    selector.flush().unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn interest_is_applied_only_on_flush() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, WRITE);
    assert_eq!(collect(&selector), vec![]);
    selector.flush().unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Writable(token)]);
}

#[test]
fn read_interest_is_one_shot() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, READ);
    selector.flush().unwrap();

    rustix::io::write(&b, b"hello").unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Readable(token)]);
    rustix::io::write(&b, b"more").unwrap();
    assert!(!is_readable(&selector, 0));
    assert_eq!(collect(&selector), vec![]);

    selector.set_interest(token, READ);
    selector.flush().unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Readable(token)]);
}

#[test]
fn data_that_arrived_before_arming_is_reported() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    rustix::io::write(&b, b"early").unwrap();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, READ);
    selector.flush().unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Readable(token)]);
}

#[test]
fn removing_armed_interest_prevents_events() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, READ);
    selector.flush().unwrap();
    selector.set_interest(token, NONE);
    selector.flush().unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
}

#[test]
fn interest_toggled_before_flush_issues_nothing() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, READ);
    selector.set_interest(token, NONE);
    selector.flush().unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn deregistered_then_closed_fd_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, READ);
    selector.flush().unwrap();
    selector.deregister(token).unwrap();
    drop(a);
    rustix::io::write(&b, b"x").ok();
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn pending_change_of_deregistered_fd_is_dropped() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, WRITE);
    selector.deregister(token).unwrap();
    drop(a);
    selector.flush().unwrap();
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn reused_fd_number_reports_the_new_token() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, WRITE);
    selector.flush().unwrap();
    selector.deregister(token).unwrap();
    drop(a);

    let (c, _d) = pair();
    let new_token = selector.register(c.as_fd());
    selector.set_interest(new_token, WRITE);
    selector.flush().unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Writable(new_token)]);
}

#[test]
fn deregister_removes_armed_filters_immediately() {
    let selector = Selector::new().unwrap();
    let (a, b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, READ);
    selector.flush().unwrap();
    selector.deregister(token).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!is_readable(&selector, 0));
}

#[test]
fn deregister_after_event_was_consumed_is_ok() {
    let selector = Selector::new().unwrap();
    let (a, _b) = pair();
    let token = selector.register(a.as_fd());
    selector.set_interest(token, WRITE);
    selector.flush().unwrap();
    assert_eq!(collect(&selector), vec![SelectorEvent::Writable(token)]);
    selector.deregister(token).unwrap();
}

#[test]
fn many_ready_fds_are_all_delivered() {
    let selector = Selector::new().unwrap();
    let pairs: Vec<_> = (0..100).map(|_| pair()).collect();
    for (a, _) in &pairs {
        let token = selector.register(a.as_fd());
        selector.set_interest(token, WRITE);
    }
    selector.arm_timer(Instant::now() + Duration::from_secs(10));
    selector.flush().unwrap();
    assert_eq!(collect(&selector).len(), pairs.len());
}

#[test]
fn timer_fires_after_flush() {
    let selector = Selector::new().unwrap();
    let start = Instant::now();
    selector.arm_timer(Instant::now() + Duration::from_millis(20));
    selector.flush().unwrap();
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
    selector.flush().unwrap();
    assert!(!is_readable(&selector, 50));

    selector.arm_timer(Instant::now() + Duration::from_millis(10));
    selector.flush().unwrap();
    assert!(is_readable(&selector, 500));
    assert_eq!(collect(&selector), vec![SelectorEvent::Timer]);
}

#[test]
fn disarm_prevents_firing() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::from_millis(10));
    selector.flush().unwrap();
    selector.disarm_timer();
    selector.flush().unwrap();
    assert!(!is_readable(&selector, 50));
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn disarm_after_timer_fired_is_ok() {
    let selector = Selector::new().unwrap();
    selector.arm_timer(Instant::now() + Duration::ZERO);
    selector.flush().unwrap();
    assert!(is_readable(&selector, 500));
    selector.disarm_timer();
    selector.flush().unwrap();
    assert_eq!(collect(&selector), vec![]);
}

#[test]
fn disarm_when_never_armed_is_ok() {
    let selector = Selector::new().unwrap();
    selector.disarm_timer();
    selector.flush().unwrap();
}
