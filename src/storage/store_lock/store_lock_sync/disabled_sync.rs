//! Disabled synchronous store mutex.

use crate::storage::StoreKey;

use super::{
    StoreKeyMutex, StoreKeyMutexGuard, StoreKeyMutexGuardTraits, StoreKeyMutexTraits,
    StoreLocksTraits,
};

/// Disabled store mutex guard.
#[derive(Debug)]
pub struct DisabledStoreMutexGuard;

impl StoreKeyMutexGuardTraits for DisabledStoreMutexGuard {}

/// Disabled store mutex.
#[derive(Debug)]
pub struct DisabledStoreMutex;

impl StoreKeyMutexTraits for DisabledStoreMutex {
    fn lock(&self) -> StoreKeyMutexGuard<'_> {
        Box::new(DisabledStoreMutexGuard)
    }
}

/// Disabled store locks.
#[derive(Debug, Default)]
pub struct DisabledStoreLocks;

impl StoreLocksTraits for DisabledStoreLocks {
    fn mutex(&self, _key: &StoreKey) -> StoreKeyMutex {
        Box::new(DisabledStoreMutex)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{atomic::AtomicUsize, Arc},
        time::Duration,
    };

    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    use crate::storage::{store::MemoryStore, ReadableWritableStorageTraits};

    use super::*;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn store_disable_lock_sync() {
        let store = MemoryStore::new_with_locks(Arc::new(DisabledStoreLocks::default()));
        let key = StoreKey::new("key").unwrap();
        let locks_held = AtomicUsize::new(0);
        assert!((0..20).into_par_iter().any(|_| {
            let mutex = store.mutex(&key).unwrap();
            let _lock = mutex.lock();
            locks_held.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(10));
            let locks_held = locks_held.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            locks_held > 1
        }));
    }
}
