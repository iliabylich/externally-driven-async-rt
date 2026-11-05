use std::collections::HashMap;
use std::collections::hash_map::Entry as MapEntry;

use rustix::fd::RawFd;

use super::token::TokenGenerator;
use super::{Interest, SelectorEvent, Token};

#[derive(Default)]
pub(super) struct Registrations {
    current: HashMap<Token, Entry>,
    draft: HashMap<Token, Entry>,
    tokens: TokenGenerator,
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    fd: RawFd,
    interest: Interest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Transition {
    pub(super) token: Token,
    pub(super) fd: RawFd,
    pub(super) armed: Interest,
    pub(super) desired: Interest,
}

impl Registrations {
    pub(super) fn register_in_memory(&mut self, fd: RawFd) -> Token {
        let token = self.tokens.next();
        self.draft.insert(
            token,
            Entry {
                fd,
                interest: Interest::default(),
            },
        );
        token
    }

    pub(super) fn deregister_now(&mut self, token: Token) -> Option<Transition> {
        self.draft.remove(&token);
        let current = self.current.remove(&token)?;
        if current.interest == Interest::default() {
            return None;
        }
        Some(Transition {
            token,
            fd: current.fd,
            armed: current.interest,
            desired: Interest::default(),
        })
    }

    pub(super) fn set_interest_in_memory(&mut self, token: Token, interest: Interest) {
        let entry = match self.draft.entry(token) {
            MapEntry::Occupied(entry) => entry.into_mut(),
            MapEntry::Vacant(entry) => {
                let Some(current) = self.current.get(&token) else {
                    return;
                };
                entry.insert(*current)
            }
        };
        entry.interest = interest;
    }

    pub(super) fn flush(&mut self) -> Vec<Transition> {
        let mut transitions = Vec::new();
        for (token, draft) in self.draft.drain() {
            let armed = self
                .current
                .get(&token)
                .map(|entry| entry.interest)
                .unwrap_or_default();
            if armed != draft.interest {
                transitions.push(Transition {
                    token,
                    fd: draft.fd,
                    armed,
                    desired: draft.interest,
                });
            }
            self.current.insert(token, draft);
        }
        transitions
    }

    pub(super) fn on_readable(&mut self, token: Token) -> Option<SelectorEvent> {
        self.current.get_mut(&token)?.interest.read = false;
        Some(SelectorEvent::Readable(token))
    }

    pub(super) fn on_writable(&mut self, token: Token) -> Option<SelectorEvent> {
        self.current.get_mut(&token)?.interest.write = false;
        Some(SelectorEvent::Writable(token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const READ: Interest = Interest {
        read: true,
        write: false,
    };
    const BOTH: Interest = Interest {
        read: true,
        write: true,
    };
    const NONE: Interest = Interest {
        read: false,
        write: false,
    };

    fn transition(token: Token, fd: RawFd, armed: Interest, desired: Interest) -> Transition {
        Transition {
            token,
            fd,
            armed,
            desired,
        }
    }

    #[test]
    fn nothing_to_do_without_interest() {
        let mut r = Registrations::default();
        r.register_in_memory(10);
        assert_eq!(r.flush(), vec![]);
    }

    #[test]
    fn interest_change_produces_one_transition() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.set_interest_in_memory(t, READ);
        assert_eq!(r.flush(), vec![transition(t, 10, NONE, READ)]);
        assert_eq!(r.flush(), vec![]);
    }

    #[test]
    fn repeated_set_interest_is_one_transition() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.set_interest_in_memory(t, READ);
        r.set_interest_in_memory(t, BOTH);
        assert_eq!(r.flush(), vec![transition(t, 10, NONE, BOTH)]);
    }

    #[test]
    fn toggled_before_flush_is_no_transition() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.set_interest_in_memory(t, READ);
        r.set_interest_in_memory(t, NONE);
        assert_eq!(r.flush(), vec![]);
    }

    #[test]
    fn deregister_drops_pending_change() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.set_interest_in_memory(t, READ);
        assert_eq!(r.deregister_now(t), None);
        assert_eq!(r.flush(), vec![]);
    }

    #[test]
    fn deregister_reports_what_is_armed() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.set_interest_in_memory(t, BOTH);
        r.flush();
        assert_eq!(r.deregister_now(t), Some(transition(t, 10, BOTH, NONE)));
        assert_eq!(r.deregister_now(t), None);
    }

    #[test]
    fn event_disarms_only_its_direction() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.set_interest_in_memory(t, BOTH);
        r.flush();
        assert_eq!(r.on_readable(t), Some(SelectorEvent::Readable(t)));
        let write_only = Interest {
            read: false,
            write: true,
        };
        assert_eq!(
            r.deregister_now(t),
            Some(transition(t, 10, write_only, NONE))
        );
    }

    #[test]
    fn rearming_after_event_produces_a_transition() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.set_interest_in_memory(t, READ);
        r.flush();
        r.on_readable(t);
        r.set_interest_in_memory(t, READ);
        assert_eq!(r.flush(), vec![transition(t, 10, NONE, READ)]);
    }

    #[test]
    fn tokens_are_never_reused() {
        let mut r = Registrations::default();
        let first = r.register_in_memory(10);
        r.deregister_now(first);
        let second = r.register_in_memory(10);
        assert_ne!(first, second);
    }

    #[test]
    fn interest_of_unknown_token_is_ignored() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.deregister_now(t);
        r.set_interest_in_memory(t, READ);
        assert_eq!(r.flush(), vec![]);
    }

    #[test]
    fn deregistered_tokens_are_ignored() {
        let mut r = Registrations::default();
        let t = r.register_in_memory(10);
        r.deregister_now(t);
        r.set_interest_in_memory(t, READ);
        assert_eq!(r.on_readable(t), None);
        assert_eq!(r.on_writable(t), None);
        assert_eq!(r.deregister_now(t), None);
        assert_eq!(r.flush(), vec![]);
    }
}
