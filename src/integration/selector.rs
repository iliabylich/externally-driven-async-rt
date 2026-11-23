use core::time::Duration;

use rustix::fd::{AsRawFd, OwnedFd};

use crate::{
    selector::{Direction, Directions, EVENTS_PER_CALL, IoChange, Selector, SelectorEvent},
    testing::{FdTestingExt, assert_yields, socketpair},
    time::Instant,
};

fn arm(fd: &OwnedFd, direction: Direction) -> IoChange {
    IoChange {
        fd: fd.as_raw_fd(),
        interest: direction.into(),
    }
}

fn arm_both(fd: &OwnedFd) -> IoChange {
    IoChange {
        fd: fd.as_raw_fd(),
        interest: Directions::Both,
    }
}

fn disarm(fd: &OwnedFd) -> IoChange {
    IoChange {
        fd: fd.as_raw_fd(),
        interest: Directions::Empty,
    }
}

fn ready(fd: &OwnedFd, direction: Direction) -> SelectorEvent {
    SelectorEvent::Io {
        fd: fd.as_raw_fd(),
        ready: direction.into(),
    }
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

fn ready_directions(selector: &Selector, fd: &OwnedFd) -> Directions {
    events(selector).fold(Directions::Empty, |directions, event| match event {
        SelectorEvent::Io {
            fd: event_fd,
            ready,
        } if event_fd == fd.as_raw_fd() => [Direction::Read, Direction::Write]
            .into_iter()
            .filter(|direction| ready.contains(*direction))
            .fold(directions, Directions::with),
        _ => directions,
    })
}

#[test]
fn nothing_armed_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (_rx, tx) = socketpair();
    selector.flush([], None, Instant::ZERO).unwrap();
    rustix::io::write(&tx, b"x").unwrap();
    assert!(!selector.as_fd().is_readable_now());
    assert_yields!(events(&selector), []);
}

#[test]
fn armed_write_is_reported_with_its_fd() {
    let selector = Selector::new().unwrap();
    let (a, _b) = socketpair();
    selector
        .flush([arm(&a, Direction::Write)], None, Instant::ZERO)
        .unwrap();
    assert_yields!(events(&selector), [ready(&a, Direction::Write)]);
}

#[test]
fn read_interest_is_one_shot() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector
        .flush([arm(&rx, Direction::Read)], None, Instant::ZERO)
        .unwrap();

    rustix::io::write(&tx, b"hello").unwrap();
    assert_yields!(events(&selector), [ready(&rx, Direction::Read)]);
    rustix::io::write(&tx, b"more").unwrap();
    assert!(!selector.as_fd().is_readable_now());
    assert_yields!(events(&selector), []);

    selector
        .flush([arm(&rx, Direction::Read)], None, Instant::ZERO)
        .unwrap();
    assert_yields!(events(&selector), [ready(&rx, Direction::Read)]);
}

#[test]
fn data_that_arrived_before_arming_is_reported() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    rustix::io::write(&tx, b"early").unwrap();
    selector
        .flush([arm(&rx, Direction::Read)], None, Instant::ZERO)
        .unwrap();
    assert_yields!(events(&selector), [ready(&rx, Direction::Read)]);
}

#[test]
fn disarming_prevents_events() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector
        .flush([arm(&rx, Direction::Read)], None, Instant::ZERO)
        .unwrap();
    selector.flush([disarm(&rx)], None, Instant::ZERO).unwrap();
    rustix::io::write(&tx, b"x").unwrap();
    assert!(!selector.as_fd().is_readable_now());
}

#[test]
fn apply_now_deletes_an_armed_filter_without_flush() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector
        .flush([arm(&rx, Direction::Read)], None, Instant::ZERO)
        .unwrap();
    selector.apply_now(disarm(&rx)).unwrap();
    rustix::io::write(&tx, b"x").unwrap();
    assert!(!selector.as_fd().is_readable_now());
}

#[test]
fn deleted_then_closed_fd_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector
        .flush([arm(&rx, Direction::Read)], None, Instant::ZERO)
        .unwrap();
    selector.apply_now(disarm(&rx)).unwrap();
    drop(rx);
    rustix::io::write(&tx, b"x").ok();
    assert_yields!(events(&selector), []);
}

#[test]
fn disarming_a_filter_that_already_fired_is_ok() {
    let selector = Selector::new().unwrap();
    let (a, _b) = socketpair();
    selector
        .flush([arm(&a, Direction::Write)], None, Instant::ZERO)
        .unwrap();
    assert_yields!(events(&selector), [ready(&a, Direction::Write)]);
    selector.apply_now(disarm(&a)).unwrap();
}

#[test]
fn reused_fd_number_reports_the_new_socket() {
    let selector = Selector::new().unwrap();
    let (a, _b) = socketpair();
    selector
        .flush([arm(&a, Direction::Write)], None, Instant::ZERO)
        .unwrap();
    selector.apply_now(disarm(&a)).unwrap();
    drop(a);

    let (c, _d) = socketpair();
    selector
        .flush([arm(&c, Direction::Write)], None, Instant::ZERO)
        .unwrap();
    assert_yields!(events(&selector), [ready(&c, Direction::Write)]);
}

#[test]
fn read_and_write_on_one_fd_are_both_reported() {
    let selector = Selector::new().unwrap();
    let (a, b) = socketpair();
    rustix::io::write(&b, b"x").unwrap();
    selector.flush([arm_both(&a)], None, Instant::ZERO).unwrap();
    assert_eq!(ready_directions(&selector, &a), Directions::Both);
}

#[test]
fn arming_one_direction_keeps_the_other() {
    let selector = Selector::new().unwrap();
    let (a, b) = socketpair();
    a.fill_send_buffer();
    selector
        .flush([arm(&a, Direction::Read)], None, Instant::ZERO)
        .unwrap();
    selector.flush([arm_both(&a)], None, Instant::ZERO).unwrap();
    assert!(!selector.as_fd().is_readable_now());

    rustix::io::write(&b, b"x").unwrap();
    assert_eq!(ready_directions(&selector, &a), Directions::Read);
}

#[test]
fn releasing_one_direction_keeps_the_other() {
    let selector = Selector::new().unwrap();
    let (a, b) = socketpair();
    a.fill_send_buffer();
    selector.flush([arm_both(&a)], None, Instant::ZERO).unwrap();
    assert!(!selector.as_fd().is_readable_now());

    selector.apply_now(arm(&a, Direction::Write)).unwrap();
    rustix::io::write(&b, b"x").unwrap();
    assert!(!selector.as_fd().is_readable_now());

    b.drain_receive_buffer();
    assert_eq!(ready_directions(&selector, &a), Directions::Write);
}

#[test]
fn changes_and_events_beyond_one_batch_are_all_delivered() {
    let selector = Selector::new().unwrap();
    let [(a0, b0), (a1, b1), (a2, b2)] = core::array::from_fn(|_| socketpair());
    let fds = [&a0, &b0, &a1, &b1, &a2, &b2];
    let arm_first = |count: usize| fds[..count].iter().map(|fd| arm(fd, Direction::Write));

    selector.flush(arm_first(4), None, Instant::ZERO).unwrap();
    assert_eq!(events(&selector).count(), 4);

    selector
        .flush(
            arm_first(fds.len()),
            Some(Instant::ZERO.saturating_add(Duration::from_secs(10))),
            Instant::ZERO,
        )
        .unwrap();
    assert_eq!(events(&selector).count(), fds.len());
}

#[test]
fn timer_fires_after_flush() {
    let selector = Selector::new().unwrap();
    let start = Instant::now();
    selector
        .flush(
            [],
            Some(Instant::ZERO.saturating_add(Duration::from_millis(20))),
            Instant::ZERO,
        )
        .unwrap();
    assert!(selector.as_fd().wait_readable_blocking(500));
    assert!(Instant::now().saturating_sub(start) >= Duration::from_millis(20));
    assert_yields!(events(&selector), [SelectorEvent::Timer]);
    assert!(!selector.as_fd().wait_readable_blocking(50));
}

#[test]
fn disarm_prevents_firing() {
    let selector = Selector::new().unwrap();
    selector
        .flush(
            [],
            Some(Instant::ZERO.saturating_add(Duration::from_millis(10))),
            Instant::ZERO,
        )
        .unwrap();
    selector.flush([], None, Instant::ZERO).unwrap();
    assert!(!selector.as_fd().wait_readable_blocking(50));
    assert_yields!(events(&selector), []);
}

#[test]
fn disarm_after_timer_fired_is_ok() {
    let selector = Selector::new().unwrap();
    selector
        .flush([], Some(Instant::ZERO), Instant::ZERO)
        .unwrap();
    assert!(selector.as_fd().wait_readable_blocking(500));
    selector.flush([], None, Instant::ZERO).unwrap();
    assert_yields!(events(&selector), []);
}

#[test]
fn disarm_when_never_armed_is_ok() {
    let selector = Selector::new().unwrap();
    selector.flush([], None, Instant::ZERO).unwrap();
}
