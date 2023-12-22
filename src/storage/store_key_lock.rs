use std::sync::Arc;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

/// Traits for a store key lock.
pub trait StoreKeyLockTraits {
    /// Locks this [`StoreKeyLock`] with shared read access, blocking the current thread until it can be acquired.
    fn read(&self) -> RwLockReadGuard<'_, ()>;

    /// Locks this [`StoreKeyLock`] with exclusive write access, blocking the current thread until it can be acquired.
    fn write(&self) -> RwLockWriteGuard<'_, ()>;
}

pub type StoreKeyLock = Arc<dyn StoreKeyLockTraits>;
