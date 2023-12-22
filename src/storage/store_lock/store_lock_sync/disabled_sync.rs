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
#[derive(Debug, Default, derive_more::Constructor)]
pub struct DisabledStoreLocks;

impl StoreLocksTraits for DisabledStoreLocks {
    fn mutex(&self, _key: &StoreKey) -> StoreKeyMutex {
        Box::new(DisabledStoreMutex)
    }
}
