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
#[derive(Debug, Default, derive_more::Constructor)]
pub struct AsyncDisabledStoreLocks;

#[async_trait::async_trait]
impl AsyncStoreLocksTraits for AsyncDisabledStoreLocks {
    async fn mutex(&self, _key: &StoreKey) -> AsyncStoreKeyMutex {
        Box::new(AsyncDisabledStoreMutex)
    }
}
