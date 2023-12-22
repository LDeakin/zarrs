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
#[derive(Debug, Default, derive_more::Constructor)]
pub struct DefaultStoreLocks(Mutex<HashMap<StoreKey, Arc<Mutex<()>>>>);

impl StoreLocksTraits for DefaultStoreLocks {
    fn mutex(&self, key: &StoreKey) -> StoreKeyMutex {
        let mut locks = self.0.lock();
        Box::new(DefaultStoreMutex(
            locks.entry(key.clone()).or_default().clone(),
        ))
    }
}
