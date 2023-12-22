//! Google Cloud stores.

use std::sync::Arc;

use crate::{
    object_store_impl,
    storage::store_lock::{AsyncDefaultStoreLocks, AsyncStoreLocks},
};

/// A Google Cloud Storage store.
#[derive(Debug)]
pub struct AsyncGoogleCloudStore {
    object_store: object_store::gcp::GoogleCloudStorage,
    locks: AsyncStoreLocks,
}

impl AsyncGoogleCloudStore {
    /// Create a new amazon S3 store.
    #[must_use]
    pub fn new(object_store: object_store::gcp::GoogleCloudStorage) -> Self {
        Self::new_with_locks(object_store, Arc::new(AsyncDefaultStoreLocks::default()))
    }

    /// Create a new amazon S3 store with non-default store locks.
    #[must_use]
    pub fn new_with_locks(
        object_store: object_store::gcp::GoogleCloudStorage,
        store_locks: AsyncStoreLocks,
    ) -> Self {
        Self {
            object_store,
            locks: store_locks,
        }
    }
}

object_store_impl!(AsyncGoogleCloudStore, object_store, locks);
