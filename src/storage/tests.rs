use crate::{LoopStorage, Selector};

type Storage = LoopStorage<(), 1, 1, 0, 0, 1, 1>;

#[test]
fn a_storage_can_only_be_claimed_once() {
    static STORAGE: Storage = Storage::new();
    let selector = Selector::new().unwrap();
    assert!(STORAGE.claim(&selector).is_some());
    assert!(STORAGE.claim(&selector).is_none());
}

#[test]
fn storage_can_be_built_at_runtime() {
    let _storage = Storage::new();
}
