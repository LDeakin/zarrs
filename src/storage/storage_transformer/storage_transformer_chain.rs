//! A sequence of storage transformers.

use derive_more::From;

use crate::{
    metadata::Metadata,
    plugin::PluginCreateError,
    storage::{ListableStorage, ReadableStorage, ReadableWritableStorage, WritableStorage},
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
    pub fn from_metadata(metadatas: &[Metadata]) -> Result<Self, PluginCreateError> {
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
        mut storage: ReadableStorage<'a>,
    ) -> ReadableStorage<'a> {
        for transformer in &self.0 {
            storage = transformer.create_readable_transformer(storage);
        }
        storage
    }

    /// Create a writable storage transformer.
    pub fn create_writable_transformer<'a>(
        &'a self,
        mut storage: WritableStorage<'a>,
    ) -> WritableStorage<'a> {
        for transformer in &self.0 {
            storage = transformer.create_writable_transformer(storage);
        }
        storage
    }

    /// Create a listable storage transformer.
    pub fn create_listable_transformer<'a>(
        &'a self,
        mut storage: ListableStorage<'a>,
    ) -> ListableStorage<'a> {
        for transformer in &self.0 {
            storage = transformer.create_listable_transformer(storage);
        }
        storage
    }

    /// Create a readable and writable storage transformer.
    pub fn create_readable_writable_transformer<'a>(
        &'a self,
        mut storage: ReadableWritableStorage<'a>,
    ) -> ReadableWritableStorage<'a> {
        for transformer in &self.0 {
            storage = transformer.create_readable_writable_transformer(storage);
        }
        storage
    }
}
