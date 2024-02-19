//! Default synchronous store mutex.

use std::{collections::HashMap, sync::Arc};

use parking_lot::{Mutex, MutexGuard};

use crate::storage::StoreKey;

use super::{
    StoreKeyMutex, StoreKeyMutexGuard, StoreKeyMutexGuardTraits, StoreKeyMutexTraits,
    StoreLocksTraits,
};

/// Default store mutex guard.
#[derive(Debug)]
#[allow(dead_code)]
pub struct DefaultStoreMutexGuard<'a>(MutexGuard<'a, ()>);

impl StoreKeyMutexGuardTraits for DefaultStoreMutexGuard<'_> {}

/// Default store mutex.
#[derive(Debug)]
pub struct DefaultStoreMutex(Arc<Mutex<()>>);

impl StoreKeyMutexTraits for DefaultStoreMutex {
    fn lock(&self) -> StoreKeyMutexGuard<'_> {
        Box::new(DefaultStoreMutexGuard::<'_>(self.0.lock()))
    }
}

/// Default store locks.
#[derive(Debug, Default)]
pub struct DefaultStoreLocks(Mutex<HashMap<StoreKey, Arc<Mutex<()>>>>);

impl StoreLocksTraits for DefaultStoreLocks {
    fn mutex(&self, key: &StoreKey) -> StoreKeyMutex {
        let mut locks = self.0.lock();
        Box::new(DefaultStoreMutex(
            locks.entry(key.clone()).or_default().clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicUsize, time::Duration};

    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    use crate::storage::{store::MemoryStore, ReadableWritableStorageTraits};

    use super::*;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn store_default_lock_sync() {
        let store = MemoryStore::new_with_locks(Arc::new(DefaultStoreLocks::default()));
        let key = StoreKey::new("key").unwrap();
        let locks_held = AtomicUsize::new(0);
        (0..20).into_par_iter().for_each(|_| {
            let mutex = store.mutex(&key).unwrap();
            let _lock = mutex.lock();
            locks_held.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(10));
            let locks_held = locks_held.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(locks_held, 1);
        });
    }
}
