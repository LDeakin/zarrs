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
#[derive(Debug, Default, derive_more::Constructor)]
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
