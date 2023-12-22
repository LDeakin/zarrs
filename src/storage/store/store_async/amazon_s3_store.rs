//! Amazon S3 stores.

use crate::{object_store_impl, storage::StorageError};

/// An Amazon S3 store.
#[derive(Debug)]
pub struct AsyncAmazonS3Store {
    object_store: object_store::aws::AmazonS3,
}

impl AsyncAmazonS3Store {
    /// Create a new amazon S3 store.
    #[must_use]
    pub fn new(object_store: object_store::aws::AmazonS3) -> Self {
        Self { object_store }
    }
}

object_store_impl!(AsyncAmazonS3Store, object_store);
