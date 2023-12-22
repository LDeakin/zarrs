//! Synchronous store mutex traits, objects and implementations.

use std::sync::Arc;

pub mod default_sync;
pub mod disabled_sync;

/// Store key lock manager.
pub type StoreLocks = Arc<dyn StoreLocksTraits>;

/// Traits for store key lock managers.
pub trait StoreLocksTraits: Send + Sync + core::fmt::Debug {
    /// Returns the mutex for the store value at `key`.
    #[must_use]
    fn mutex(&self, key: &crate::storage::StoreKey) -> StoreKeyMutex;
}

/// Mutex guard for a store object.
pub type StoreKeyMutex = Box<dyn StoreKeyMutexTraits>;

/// Traits for a store key mutex.
pub trait StoreKeyMutexTraits {
    /// Acquires a mutex, blocking the current thread until it is able to do so.
    ///
    /// When the returned guard goes out of scope, the mutex will be unlocked.
    #[must_use]
    fn lock(&self) -> StoreKeyMutexGuard<'_>;
}

/// Store key mutex guard.
pub type StoreKeyMutexGuard<'a> = Box<dyn StoreKeyMutexGuardTraits + 'a>;

/// Traits for a store key mutex guard.
pub trait StoreKeyMutexGuardTraits {}
