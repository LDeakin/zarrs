//! Asynchronous store mutex traits, objects and implementations.

use std::sync::Arc;

pub mod default_async;
pub mod disabled_async;

/// Asynchronous store key lock manager.
pub type AsyncStoreLocks = Arc<dyn AsyncStoreLocksTraits>;

/// Traits for asynchronous store key lock managers.
#[async_trait::async_trait]
pub trait AsyncStoreLocksTraits: Send + Sync + core::fmt::Debug {
    /// Returns the mutex for the store value at `key`.
    #[must_use]
    async fn mutex(&self, key: &crate::storage::StoreKey) -> AsyncStoreKeyMutex;
}

/// Mutex guard for a store object.
pub type AsyncStoreKeyMutex = Box<dyn AsyncStoreKeyMutexTraits>;

/// Traits for a store key mutex.
#[async_trait::async_trait]
pub trait AsyncStoreKeyMutexTraits: Send + Sync {
    /// Acquires a mutex, blocking the current thread until it is able to do so.
    ///
    /// When the returned guard goes out of scope, the mutex will be unlocked.
    #[must_use]
    async fn lock(&self) -> AsyncStoreKeyMutexGuard<'_>;
}

/// Store key mutex guard.
pub type AsyncStoreKeyMutexGuard<'a> = Box<dyn AsyncStoreKeyMutexGuardTraits + 'a>;

/// Traits for a store key mutex guard.
pub trait AsyncStoreKeyMutexGuardTraits: Send + Sync {}
