//! Amazon S3 stores.

use std::sync::Arc;

use crate::{
    object_store_impl,
    storage::store_lock::{AsyncDefaultStoreLocks, AsyncStoreLocks},
};

/// An Amazon S3 store.
#[derive(Debug)]
pub struct AsyncAmazonS3Store {
    object_store: object_store::aws::AmazonS3,
    locks: AsyncStoreLocks,
}

impl AsyncAmazonS3Store {
    /// Create a new amazon S3 store.
    #[must_use]
    pub fn new(object_store: object_store::aws::AmazonS3) -> Self {
        Self::new_with_locks(object_store, Arc::new(AsyncDefaultStoreLocks::default()))
    }

    /// Create a new amazon S3 store with non-default store locks.
    #[must_use]
    pub fn new_with_locks(
        object_store: object_store::aws::AmazonS3,
        store_locks: AsyncStoreLocks,
    ) -> Self {
        Self {
            object_store,
            locks: store_locks,
        }
    }
}

object_store_impl!(AsyncAmazonS3Store, object_store, locks);
