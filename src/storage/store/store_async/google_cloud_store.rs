//! Google Cloud stores.

use crate::{object_store_impl, storage::StorageError};

/// A Google Cloud Storage store.
#[derive(Debug)]
pub struct AsyncGoogleCloudStore {
    object_store: object_store::gcp::GoogleCloudStorage,
}

impl AsyncGoogleCloudStore {
    /// Create a new amazon S3 store.
    #[must_use]
    pub fn new(object_store: object_store::gcp::GoogleCloudStorage) -> Self {
        Self { object_store }
    }
}

object_store_impl!(AsyncGoogleCloudStore, object_store);
