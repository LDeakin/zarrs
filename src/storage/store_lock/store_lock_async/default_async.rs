//! Default asynchronous store mutex.

use std::{collections::HashMap, sync::Arc};

use async_lock::{Mutex, MutexGuard};

use crate::storage::StoreKey;

use super::{
    AsyncStoreKeyMutex, AsyncStoreKeyMutexGuard, AsyncStoreKeyMutexGuardTraits,
    AsyncStoreKeyMutexTraits, AsyncStoreLocksTraits,
};

/// Default store mutex guard.
#[derive(Debug)]
#[allow(dead_code)]
pub struct AsyncDefaultStoreMutexGuard<'a>(MutexGuard<'a, ()>);

impl AsyncStoreKeyMutexGuardTraits for AsyncDefaultStoreMutexGuard<'_> {}

/// Default store mutex.
#[derive(Debug)]
pub struct AsyncDefaultStoreMutex(Arc<Mutex<()>>);

#[async_trait::async_trait]
impl AsyncStoreKeyMutexTraits for AsyncDefaultStoreMutex {
    async fn lock(&self) -> AsyncStoreKeyMutexGuard<'_> {
        Box::new(AsyncDefaultStoreMutexGuard::<'_>(self.0.lock().await))
    }
}

/// Default store locks.
#[derive(Debug, Default)]
pub struct AsyncDefaultStoreLocks(Mutex<HashMap<StoreKey, Arc<Mutex<()>>>>);

#[async_trait::async_trait]
impl AsyncStoreLocksTraits for AsyncDefaultStoreLocks {
    async fn mutex(&self, key: &StoreKey) -> AsyncStoreKeyMutex {
        let mut locks = self.0.lock().await;
        Box::new(AsyncDefaultStoreMutex(
            locks.entry(key.clone()).or_default().clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicUsize, time::Duration};

    use crate::storage::{store::AsyncObjectStore, AsyncReadableWritableStorageTraits};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg_attr(miri, ignore)]
    async fn store_default_lock_async() {
        let store = Arc::new(AsyncObjectStore::new_with_locks(
            object_store::memory::InMemory::default(),
            Arc::new(AsyncDefaultStoreLocks::default()),
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
                locks_held == 1
            })
        });
        let result = futures::future::try_join_all(futures).await.unwrap();
        println!("{result:?}");
        assert!(result.iter().all(|b| *b));
    }
}
