//! A sequence of storage transformers.

use derive_more::From;

use crate::{
    metadata::v3::MetadataV3,
    plugin::PluginCreateError,
    storage::{
        ListableStorage, ReadableListableStorage, ReadableStorage, ReadableWritableStorage,
        WritableStorage,
    },
};

#[cfg(feature = "async")]
use crate::storage::{
    AsyncListableStorage, AsyncReadableListableStorage, AsyncReadableStorage, AsyncWritableStorage,
};

use super::{try_create_storage_transformer, StorageTransformer};

/// Configuration for a storage transformer chain.
#[derive(Debug, Clone, Default, From)]
pub struct StorageTransformerChain(Vec<StorageTransformer>);

impl StorageTransformerChain {
    /// Create a storage transformer chain from a list of storage transformers.
    #[must_use]
    pub fn new(storage_transformers: Vec<StorageTransformer>) -> Self {
        Self(storage_transformers)
    }

    /// Create a storage transformer chain from configurations.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCreateError`] if there is a configuration issue or attempt to create an unregistered storage transformer.
    pub fn from_metadata(metadatas: &[MetadataV3]) -> Result<Self, PluginCreateError> {
        let mut storage_transformers = Vec::with_capacity(metadatas.len());
        for metadata in metadatas {
            let storage_transformer = try_create_storage_transformer(metadata)?;
            storage_transformers.push(storage_transformer);
        }
        Ok(Self(storage_transformers))
    }

    /// Create storage transformer chain metadata.
    #[must_use]
    pub fn create_metadatas(&self) -> Vec<MetadataV3> {
        self.0
            .iter()
            .filter_map(|storage_transformer| storage_transformer.create_metadata())
            .collect()
    }
}

impl StorageTransformerChain {
    /// Create a readable storage transformer.
    pub fn create_readable_transformer(&self, mut storage: ReadableStorage) -> ReadableStorage {
        for transformer in &self.0 {
            storage = transformer.clone().create_readable_transformer(storage);
        }
        storage
    }

    /// Create a writable storage transformer.
    pub fn create_writable_transformer(&self, mut storage: WritableStorage) -> WritableStorage {
        for transformer in &self.0 {
            storage = transformer.clone().create_writable_transformer(storage);
        }
        storage
    }

    /// Create a readable and writable storage transformer.
    pub fn create_readable_writable_transformer(
        &self,
        mut storage: ReadableWritableStorage,
    ) -> ReadableWritableStorage {
        for transformer in &self.0 {
            storage = transformer
                .clone()
                .create_readable_writable_transformer(storage);
        }
        storage
    }

    /// Create a listable storage transformer.
    pub fn create_listable_transformer(&self, mut storage: ListableStorage) -> ListableStorage {
        for transformer in &self.0 {
            storage = transformer.clone().create_listable_transformer(storage);
        }
        storage
    }

    /// Create a readable and listable storage transformer.
    pub fn create_readable_listable_transformer(
        &self,
        mut storage: ReadableListableStorage,
    ) -> ReadableListableStorage {
        for transformer in &self.0 {
            storage = transformer
                .clone()
                .create_readable_listable_transformer(storage);
        }
        storage
    }

    #[cfg(feature = "async")]
    /// Create an asynchronous readable storage transformer.
    pub fn create_async_readable_transformer(
        &self,
        mut storage: AsyncReadableStorage,
    ) -> AsyncReadableStorage {
        for transformer in &self.0 {
            storage = transformer
                .clone()
                .create_async_readable_transformer(storage);
        }
        storage
    }

    #[cfg(feature = "async")]
    /// Create an asynchronous writable storage transformer.
    pub fn create_async_writable_transformer(
        &self,
        mut storage: AsyncWritableStorage,
    ) -> AsyncWritableStorage {
        for transformer in &self.0 {
            storage = transformer
                .clone()
                .create_async_writable_transformer(storage);
        }
        storage
    }

    #[cfg(feature = "async")]
    /// Create an asynchronous listable storage transformer.
    pub fn create_async_listable_transformer(
        &self,
        mut storage: AsyncListableStorage,
    ) -> AsyncListableStorage {
        for transformer in &self.0 {
            storage = transformer
                .clone()
                .create_async_listable_transformer(storage);
        }
        storage
    }

    #[cfg(feature = "async")]
    /// Create an asynchronous readable listable storage transformer.
    pub fn create_async_readable_listable_transformer(
        &self,
        mut storage: AsyncReadableListableStorage,
    ) -> AsyncReadableListableStorage {
        for transformer in &self.0 {
            storage = transformer
                .clone()
                .create_async_readable_listable_transformer(storage);
        }
        storage
    }
}
