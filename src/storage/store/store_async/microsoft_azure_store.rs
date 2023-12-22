//! Azure blob storage stores.

use std::sync::Arc;

use crate::{
    object_store_impl,
    storage::store_lock::{AsyncDefaultStoreLocks, AsyncStoreLocks},
};

/// A Microsoft Azure store.
#[derive(Debug)]
pub struct AsyncMicrosoftAzureStore {
    object_store: object_store::azure::MicrosoftAzure,
    locks: AsyncStoreLocks,
}

impl AsyncMicrosoftAzureStore {
    /// Create a new amazon S3 store.
    #[must_use]
    pub fn new(object_store: object_store::azure::MicrosoftAzure) -> Self {
        Self::new_with_locks(object_store, Arc::new(AsyncDefaultStoreLocks::default()))
    }

    /// Create a new amazon S3 store.
    #[must_use]
    pub fn new_with_locks(
        object_store: object_store::azure::MicrosoftAzure,
        store_locks: AsyncStoreLocks,
    ) -> Self {
        Self {
            object_store,
            locks: store_locks,
        }
    }
}

object_store_impl!(AsyncMicrosoftAzureStore, object_store, locks);
