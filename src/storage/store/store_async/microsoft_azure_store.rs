//! Azure blob storage stores.

use crate::object_store_impl;

/// A Microsoft Azure store.
#[derive(Debug)]
pub struct AsyncMicrosoftAzureStore {
    object_store: object_store::azure::MicrosoftAzure,
}

impl AsyncMicrosoftAzureStore {
    /// Create a new amazon S3 store.
    #[must_use]
    pub fn new(object_store: object_store::azure::MicrosoftAzure) -> Self {
        Self { object_store }
    }
}

object_store_impl!(AsyncMicrosoftAzureStore, object_store);
