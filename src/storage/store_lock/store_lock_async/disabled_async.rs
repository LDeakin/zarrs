//! Disabled asynchronous store mutex.

use crate::storage::StoreKey;

use super::{
    AsyncStoreKeyMutex, AsyncStoreKeyMutexGuard, AsyncStoreKeyMutexGuardTraits,
    AsyncStoreKeyMutexTraits, AsyncStoreLocksTraits,
};

/// Disabled store mutex guard.
#[derive(Debug)]
pub struct AsyncDisabledStoreMutexGuard;

impl AsyncStoreKeyMutexGuardTraits for AsyncDisabledStoreMutexGuard {}

/// Disabled store mutex.
#[derive(Debug)]
pub struct AsyncDisabledStoreMutex;

#[async_trait::async_trait]
impl AsyncStoreKeyMutexTraits for AsyncDisabledStoreMutex {
    async fn lock(&self) -> AsyncStoreKeyMutexGuard<'_> {
        Box::new(AsyncDisabledStoreMutexGuard)
    }
}

/// Disabled store locks.
#[derive(Debug, Default)]
pub struct AsyncDisabledStoreLocks;

#[async_trait::async_trait]
impl AsyncStoreLocksTraits for AsyncDisabledStoreLocks {
    async fn mutex(&self, _key: &StoreKey) -> AsyncStoreKeyMutex {
        Box::new(AsyncDisabledStoreMutex)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{atomic::AtomicUsize, Arc},
        time::Duration,
    };

    use crate::storage::{store::AsyncObjectStore, AsyncReadableWritableStorageTraits};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(miri, ignore)]
    async fn store_disabled_lock_async() {
        let store = Arc::new(AsyncObjectStore::new_with_locks(
            object_store::memory::InMemory::default(),
            Arc::new(AsyncDisabledStoreLocks::default()),
        ));
        let locks_held = Arc::new(AtomicUsize::new(0));
        let futures = (0..20).into_iter().map(|_| {
            let key = StoreKey::new("key").unwrap();
            let store = store.clone();
            let locks_held = locks_held.clone();
            tokio::task::spawn(async move {
                let mutex = store.mutex(&key).await.unwrap();
                let _lock = mutex.lock().await;
                locks_held.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(10));
                let locks_held = locks_held.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                locks_held > 1
            })
        });
        let result = futures::future::try_join_all(futures).await.unwrap();
        println!("{result:?}");
        assert!(result.iter().any(|b| *b));
    }
}
