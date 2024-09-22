//! Zarr storage transformers.
//!
//! A Zarr storage transformer modifies a request to read or write data before passing that request to a following storage transformer or store.
//! A [`StorageTransformerChain`] represents a sequence of storage transformers.
//! A storage transformer chain and individual storage transformers all have the same interface as a [store](crate::storage::store).
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id23>.

mod storage_transformer_chain;
pub use storage_transformer_chain::StorageTransformerChain;

mod storage_transformer_plugin;
pub use storage_transformer_plugin::StorageTransformerPlugin;

use std::sync::Arc;

use crate::{
    metadata::v3::MetadataV3,
    node::NodePath,
    plugin::PluginCreateError,
    storage::{ListableStorage, ReadableStorage, StorageError, WritableStorage},
};

#[cfg(feature = "async")]
use crate::storage::{AsyncListableStorage, AsyncReadableStorage, AsyncWritableStorage};

/// An [`Arc`] wrapped storage transformer.
pub type StorageTransformer = Arc<dyn StorageTransformerExtension>;

/// Create a storage transformer from metadata.
///
/// # Errors
///
/// Returns [`PluginCreateError`] if the metadata is invalid or not associated with a registered storage transformer plugin.
pub fn try_create_storage_transformer(
    metadata: &MetadataV3,
    path: &NodePath,
) -> Result<StorageTransformer, PluginCreateError> {
    for plugin in inventory::iter::<StorageTransformerPlugin> {
        if plugin.match_name(metadata.name()) {
            return plugin.create(metadata, path);
        }
    }
    Err(PluginCreateError::Unsupported {
        name: metadata.name().to_string(),
        plugin_type: "storage transformer".to_string(),
    })
}

/// A storage transformer extension.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait StorageTransformerExtension: core::fmt::Debug + Send + Sync {
    /// Create metadata.
    fn create_metadata(&self) -> MetadataV3;

    /// Create a readable transformer.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    fn create_readable_transformer(
        self: Arc<Self>,
        storage: ReadableStorage,
    ) -> Result<ReadableStorage, StorageError>;

    /// Create a writable transformer.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    fn create_writable_transformer(
        self: Arc<Self>,
        storage: WritableStorage,
    ) -> Result<WritableStorage, StorageError>;

    /// Create a listable transformer.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    fn create_listable_transformer(
        self: Arc<Self>,
        storage: ListableStorage,
    ) -> Result<ListableStorage, StorageError>;

    #[cfg(feature = "async")]
    /// Create an asynchronous readable transformer.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    async fn create_async_readable_transformer(
        self: Arc<Self>,
        storage: AsyncReadableStorage,
    ) -> Result<AsyncReadableStorage, StorageError>;

    #[cfg(feature = "async")]
    /// Create an asynchronous writable transformer.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    async fn create_async_writable_transformer(
        self: Arc<Self>,
        storage: AsyncWritableStorage,
    ) -> Result<AsyncWritableStorage, StorageError>;

    #[cfg(feature = "async")]
    /// Create an asynchronous listable transformer.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    async fn create_async_listable_transformer(
        self: Arc<Self>,
        storage: AsyncListableStorage,
    ) -> Result<AsyncListableStorage, StorageError>;
}
