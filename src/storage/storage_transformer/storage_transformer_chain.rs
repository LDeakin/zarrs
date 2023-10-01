//! A sequence of storage transformers.

use std::sync::Arc;

use derive_more::From;

use crate::{
    metadata::Metadata,
    plugin::PluginCreateError,
    storage::{
        ListableStorage, ListableStorageTraits, ReadableStorage, ReadableStorageTraits,
        ReadableWritableStorage, ReadableWritableStorageTraits, StorageError, WritableStorage,
        WritableStorageTraits,
    },
};

use super::{try_create_storage_transformer, StorageTransformer};

/// Configuration for a storage transformer chain.
#[derive(Debug, Default, From)]
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
    pub fn new_with_metadatas(metadatas: &[Metadata]) -> Result<Self, PluginCreateError> {
        let mut storage_transformers = Vec::with_capacity(metadatas.len());
        for metadata in metadatas {
            let storage_transformer = try_create_storage_transformer(metadata)?;
            storage_transformers.push(storage_transformer);
        }
        Ok(Self(storage_transformers))
    }

    /// Create storage transformer chain metadata.
    #[must_use]
    pub fn create_metadatas(&self) -> Vec<Metadata> {
        self.0
            .iter()
            .filter_map(|storage_transformer| storage_transformer.create_metadata())
            .collect()
    }
}

impl StorageTransformerChain {
    /// Create a readable storage transformer.
    pub fn create_readable_transformer<'a>(
        &'a self,
        storage: &'a dyn ReadableStorageTraits,
    ) -> ReadableStorage<'a> {
        let mut storage: ReadableStorage<'a> = Arc::new(ReadableStorageHandle(storage));
        for transformer in &self.0 {
            storage = transformer.create_readable_transformer(storage);
        }
        storage
    }

    /// Create a writable storage transformer.
    pub fn create_writable_transformer<'a>(
        &'a self,
        storage: &'a dyn WritableStorageTraits,
    ) -> WritableStorage<'a> {
        let mut storage: WritableStorage<'a> = Arc::new(WritableStorageHandle(storage));
        for transformer in &self.0 {
            storage = transformer.create_writable_transformer(storage);
        }
        storage
    }

    /// Create a listable storage transformer.
    pub fn create_listable_transformer<'a>(
        &'a self,
        storage: &'a dyn ListableStorageTraits,
    ) -> ListableStorage<'a> {
        let mut storage: ListableStorage<'a> = Arc::new(ListableStorageHandle(storage));
        for transformer in &self.0 {
            storage = transformer.create_listable_transformer(storage);
        }
        storage
    }

    /// Create a readable and writable storage transformer.
    pub fn create_readable_writable_transformer<'a>(
        &'a self,
        storage: &'a dyn ReadableWritableStorageTraits,
    ) -> ReadableWritableStorage<'a> {
        let mut storage: ReadableWritableStorage<'a> =
            Arc::new(ReadableWritableStorageHandle(storage));
        for transformer in &self.0 {
            storage = transformer.create_readable_writable_transformer(storage);
        }
        storage
    }
}

#[derive(Debug)]
struct ReadableStorageHandle<'a>(&'a dyn ReadableStorageTraits);

impl ReadableStorageTraits for ReadableStorageHandle<'_> {
    fn get(&self, key: &crate::storage::StoreKey) -> Result<Vec<u8>, StorageError> {
        self.0.get(key)
    }

    fn get_partial_values(
        &self,
        key_ranges: &[crate::storage::StoreKeyRange],
    ) -> Vec<Result<Vec<u8>, StorageError>> {
        self.0.get_partial_values(key_ranges)
    }

    fn size(&self) -> u64 {
        self.0.size()
    }
}

#[derive(Debug)]
struct WritableStorageHandle<'a>(&'a dyn WritableStorageTraits);

impl WritableStorageTraits for WritableStorageHandle<'_> {
    fn set(&self, key: &crate::storage::StoreKey, value: &[u8]) -> Result<(), StorageError> {
        self.0.set(key, value)
    }

    fn set_partial_values(
        &self,
        key_start_values: &[crate::storage::StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        self.0.set_partial_values(key_start_values)
    }

    fn erase(&self, key: &crate::storage::StoreKey) -> Result<(), StorageError> {
        self.0.erase(key)
    }

    fn erase_values(&self, keys: &[crate::storage::StoreKey]) -> Result<(), StorageError> {
        self.0.erase_values(keys)
    }

    fn erase_prefix(&self, prefix: &crate::storage::StorePrefix) -> Result<(), StorageError> {
        self.0.erase_prefix(prefix)
    }
}

#[derive(Debug)]
struct ListableStorageHandle<'a>(&'a dyn ListableStorageTraits);

impl ListableStorageTraits for ListableStorageHandle<'_> {
    fn list(&self) -> Result<crate::storage::StoreKeys, StorageError> {
        self.0.list()
    }

    fn list_prefix(
        &self,
        prefix: &crate::storage::StorePrefix,
    ) -> Result<crate::storage::StoreKeys, StorageError> {
        self.0.list_prefix(prefix)
    }

    fn list_dir(
        &self,
        prefix: &crate::storage::StorePrefix,
    ) -> Result<crate::storage::StoreKeysPrefixes, StorageError> {
        self.0.list_dir(prefix)
    }
}

#[derive(Debug)]
struct ReadableWritableStorageHandle<'a>(&'a dyn ReadableWritableStorageTraits);

impl ReadableStorageTraits for ReadableWritableStorageHandle<'_> {
    fn get(&self, key: &crate::storage::StoreKey) -> Result<Vec<u8>, StorageError> {
        self.0.get(key)
    }

    fn get_partial_values(
        &self,
        key_ranges: &[crate::storage::StoreKeyRange],
    ) -> Vec<Result<Vec<u8>, StorageError>> {
        self.0.get_partial_values(key_ranges)
    }

    fn size(&self) -> u64 {
        self.0.size()
    }
}

impl WritableStorageTraits for ReadableWritableStorageHandle<'_> {
    fn set(&self, key: &crate::storage::StoreKey, value: &[u8]) -> Result<(), StorageError> {
        self.0.set(key, value)
    }

    fn set_partial_values(
        &self,
        key_start_values: &[crate::storage::StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        self.0.set_partial_values(key_start_values)
    }

    fn erase(&self, key: &crate::storage::StoreKey) -> Result<(), StorageError> {
        self.0.erase(key)
    }

    fn erase_values(&self, keys: &[crate::storage::StoreKey]) -> Result<(), StorageError> {
        self.0.erase_values(keys)
    }

    fn erase_prefix(&self, prefix: &crate::storage::StorePrefix) -> Result<(), StorageError> {
        self.0.erase_prefix(prefix)
    }
}

impl ReadableWritableStorageTraits for ReadableWritableStorageHandle<'_> {}
