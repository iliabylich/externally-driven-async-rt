use core::time::Duration;

use rustix::fd::{AsFd, AsRawFd, OwnedFd};

use crate::{
    selector::{Direction, EVENTS_PER_CALL, Selector, SelectorEvent},
    testing::{FdTestingExt, assert_yields},
    time::Instant,
};

use super::{FdExt, socketpair};

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
            full = selector.poll(&mut batch, Some(Duration::ZERO)).unwrap();
            next = 0;
        }
    })
}

fn readable(selector: &Selector) -> impl Iterator<Item = i32> + '_ {
    events(selector).filter_map(|event| match event {
        SelectorEvent::Io { fd, ready } if ready.contains(Direction::Read) => Some(fd),
        _ => None,
    })
}

#[test]
fn data_after_registration_is_reported_with_its_fd() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector.register(rx.as_fd()).unwrap();
    assert_yields!(readable(&selector), []);
    rustix::io::write(&tx, b"x").unwrap();
    assert_yields!(readable(&selector), [rx.as_raw_fd()]);
    selector.deregister(rx.as_fd()).unwrap();
}

#[test]
fn an_edge_is_reported_once_and_new_data_is_a_new_edge() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector.register(rx.as_fd()).unwrap();
    rustix::io::write(&tx, b"hello").unwrap();
    assert_yields!(readable(&selector), [rx.as_raw_fd()]);
    assert!(!selector.as_fd().is_readable_now());
    assert_yields!(readable(&selector), []);

    rustix::io::write(&tx, b"more").unwrap();
    assert_yields!(readable(&selector), [rx.as_raw_fd()]);
    selector.deregister(rx.as_fd()).unwrap();
}

#[test]
fn a_full_send_buffer_reports_writable_once_drained() {
    let selector = Selector::new().unwrap();
    let (a, b) = socketpair();
    let fill = [0; 4096];
    while rustix::io::write(&a, &fill).is_ok() {}
    selector.register(a.as_fd()).unwrap();
    let _ = events(&selector).count();

    let mut buf = [0; 4096];
    while rustix::io::read(&b, &mut buf).is_ok() {}
    assert!(events(&selector).any(|event| matches!(
        event,
        SelectorEvent::Io { fd, ready } if fd == a.as_raw_fd() && ready.contains(Direction::Write)
    )));
    selector.deregister(a.as_fd()).unwrap();
}

#[test]
fn peer_close_is_reported_as_readable() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector.register(rx.as_fd()).unwrap();
    let _ = events(&selector).count();
    drop(tx);
    assert_yields!(readable(&selector), [rx.as_raw_fd()]);
    selector.deregister(rx.as_fd()).unwrap();
}

#[test]
fn deregistered_fd_produces_no_events() {
    let selector = Selector::new().unwrap();
    let (rx, tx) = socketpair();
    selector.register(rx.as_fd()).unwrap();
    let _ = events(&selector).count();
    selector.deregister(rx.as_fd()).unwrap();
    rustix::io::write(&tx, b"x").unwrap();
    assert!(!selector.as_fd().is_readable_now());
    assert_yields!(events(&selector), []);
}

#[test]
fn reused_fd_number_reports_the_new_socket() {
    let selector = Selector::new().unwrap();
    let (a, _b) = socketpair();
    selector.register(a.as_fd()).unwrap();
    selector.deregister(a.as_fd()).unwrap();
    drop(a);

    let (c, d) = socketpair();
    selector.register(c.as_fd()).unwrap();
    let _ = events(&selector).count();
    rustix::io::write(&d, b"x").unwrap();
    assert_yields!(readable(&selector), [c.as_raw_fd()]);
    selector.deregister(c.as_fd()).unwrap();
}

#[test]
fn events_beyond_one_batch_are_all_delivered() {
    let selector = Selector::new().unwrap();
    let pairs: [(OwnedFd, OwnedFd); 6] = core::array::from_fn(|_| socketpair());
    for (rx, _) in &pairs {
        selector.register(rx.as_fd()).unwrap();
    }
    let _ = events(&selector).count();
    for (_, tx) in &pairs {
        rustix::io::write(tx, b"x").unwrap();
    }
    assert_eq!(readable(&selector).count(), pairs.len());
    for (rx, _) in &pairs {
        selector.deregister(rx.as_fd()).unwrap();
    }
}

#[test]
fn timer_fires_after_flush() {
    let selector = Selector::new().unwrap();
    let start = Instant::now();
    selector
        .flush(
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
            Some(Instant::ZERO.saturating_add(Duration::from_millis(10))),
            Instant::ZERO,
        )
        .unwrap();
    selector.flush(None, Instant::ZERO).unwrap();
    assert!(!selector.as_fd().wait_readable_blocking(50));
    assert_yields!(events(&selector), []);
}

#[test]
fn disarm_after_timer_fired_is_ok() {
    let selector = Selector::new().unwrap();
    selector.flush(Some(Instant::ZERO), Instant::ZERO).unwrap();
    assert!(selector.as_fd().wait_readable_blocking(500));
    selector.flush(None, Instant::ZERO).unwrap();
    assert_yields!(events(&selector), []);
}

#[test]
fn disarm_when_never_armed_is_ok() {
    let selector = Selector::new().unwrap();
    selector.flush(None, Instant::ZERO).unwrap();
}
