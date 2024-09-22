//! Zarr storage transformers.
//!
//! A Zarr storage transformer modifies a request to read or write data before passing that request to a following storage transformer or store.
//! A [`StorageTransformerChain`] represents a sequence of storage transformers.
//! A storage transformer chain and individual storage transformers all have the same interface as a [store](crate::storage::store).
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id23>.

mod storage_transformer_chain;
pub use storage_transformer_chain::StorageTransformerChain;

use std::sync::Arc;

use crate::{
    metadata::v3::MetadataV3,
    plugin::{Plugin, PluginCreateError},
    storage::{
        ListableStorage, ReadableListableStorage, ReadableStorage, ReadableWritableListableStorage,
        ReadableWritableStorage, WritableStorage,
    },
};

#[cfg(feature = "async")]
use crate::storage::{
    AsyncListableStorage, AsyncReadableListableStorage, AsyncReadableStorage,
    AsyncReadableWritableListableStorage, AsyncWritableStorage,
};

/// An [`Arc`] wrapped storage transformer.
pub type StorageTransformer = Arc<dyn StorageTransformerExtension>;

/// A storage transformer plugin.
type StorageTransformerPlugin = Plugin<StorageTransformer>;
inventory::collect!(StorageTransformerPlugin);

/// Create a storage transformer from metadata.
///
/// # Errors
///
/// Returns [`PluginCreateError`] if the metadata is invalid or not associated with a registered storage transformer plugin.
pub fn try_create_storage_transformer(
    metadata: &MetadataV3,
) -> Result<StorageTransformer, PluginCreateError> {
    for plugin in inventory::iter::<StorageTransformerPlugin> {
        if plugin.match_name(metadata.name()) {
            return plugin.create(metadata);
        }
    }
    Err(PluginCreateError::Unsupported {
        name: metadata.name().to_string(),
        plugin_type: "storage transformer".to_string(),
    })
}

/// A storage transformer extension.
pub trait StorageTransformerExtension: core::fmt::Debug + Send + Sync {
    /// Create metadata.
    fn create_metadata(&self) -> Option<MetadataV3>;

    /// Create a readable transformer.
    fn create_readable_transformer(self: Arc<Self>, storage: ReadableStorage) -> ReadableStorage;

    /// Create a writable transformer.
    fn create_writable_transformer(self: Arc<Self>, storage: WritableStorage) -> WritableStorage;

    /// Create a readable and writable transformer.
    fn create_readable_writable_transformer(
        self: Arc<Self>,
        storage: ReadableWritableStorage,
    ) -> ReadableWritableStorage;

    /// Create a listable transformer.
    fn create_listable_transformer(self: Arc<Self>, storage: ListableStorage) -> ListableStorage;

    /// Create a readable and listable transformer.
    fn create_readable_listable_transformer(
        self: Arc<Self>,
        storage: ReadableListableStorage,
    ) -> ReadableListableStorage;

    /// Create a readable, writable, and listable transformer.
    fn create_readable_writable_listable_transformer(
        self: Arc<Self>,
        storage: ReadableWritableListableStorage,
    ) -> ReadableWritableListableStorage;

    #[cfg(feature = "async")]
    /// Create an asynchronous readable transformer.
    fn create_async_readable_transformer(
        self: Arc<Self>,
        storage: AsyncReadableStorage,
    ) -> AsyncReadableStorage;

    #[cfg(feature = "async")]
    /// Create an asynchronous writable transformer.
    fn create_async_writable_transformer(
        self: Arc<Self>,
        storage: AsyncWritableStorage,
    ) -> AsyncWritableStorage;

    #[cfg(feature = "async")]
    /// Create an asynchronous listable transformer.
    fn create_async_listable_transformer(
        self: Arc<Self>,
        storage: AsyncListableStorage,
    ) -> AsyncListableStorage;

    #[cfg(feature = "async")]
    /// Create an asynchronous readable and listable transformer.
    fn create_async_readable_listable_transformer(
        self: Arc<Self>,
        storage: AsyncReadableListableStorage,
    ) -> AsyncReadableListableStorage;

    #[cfg(feature = "async")]
    /// Create an asynchronous readable, writable, and listable transformer.
    fn create_async_readable_writable_listable_transformer(
        self: Arc<Self>,
        storage: AsyncReadableWritableListableStorage,
    ) -> AsyncReadableWritableListableStorage;
}
