//! Zarr storage transformers. Includes [performance metrics](performance_metrics::PerformanceMetricsStorageTransformer) and [usage log](usage_log::UsageLogStorageTransformer) implementations for internal use.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id23>.

mod performance_metrics;
mod storage_transformer_chain;
mod usage_log;

pub use performance_metrics::PerformanceMetricsStorageTransformer;
pub use storage_transformer_chain::StorageTransformerChain;
pub use usage_log::UsageLogStorageTransformer;

use std::sync::Arc;

use crate::{
    metadata::Metadata,
    plugin::{Plugin, PluginCreateError},
};

use super::{
    ListableStorage, ReadableListableStorage, ReadableStorage, ReadableWritableStorage,
    WritableStorage,
};

#[cfg(feature = "async")]
use super::{
    AsyncListableStorage, AsyncReadableListableStorage, AsyncReadableStorage, AsyncWritableStorage,
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
    metadata: &Metadata,
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
    fn create_metadata(&self) -> Option<Metadata>;

    /// Create a readable transformer.
    fn create_readable_transformer<'a>(
        &'a self,
        storage: ReadableStorage<'a>,
    ) -> ReadableStorage<'a>;

    /// Create a writable transformer.
    fn create_writable_transformer<'a>(
        &'a self,
        storage: WritableStorage<'a>,
    ) -> WritableStorage<'a>;

    /// Create a readable and writable transformer.
    fn create_readable_writable_transformer<'a>(
        &'a self,
        storage: ReadableWritableStorage<'a>,
    ) -> ReadableWritableStorage<'a>;

    /// Create a listable transformer.
    fn create_listable_transformer<'a>(
        &'a self,
        storage: ListableStorage<'a>,
    ) -> ListableStorage<'a>;

    /// Create a readable and listable transformer.
    fn create_readable_listable_transformer<'a>(
        &'a self,
        storage: ReadableListableStorage<'a>,
    ) -> ReadableListableStorage<'a>;

    #[cfg(feature = "async")]
    /// Create an asynchronous readable transformer.
    fn create_async_readable_transformer<'a>(
        &'a self,
        storage: AsyncReadableStorage<'a>,
    ) -> AsyncReadableStorage<'a>;

    #[cfg(feature = "async")]
    /// Create an asynchronous writable transformer.
    fn create_async_writable_transformer<'a>(
        &'a self,
        storage: AsyncWritableStorage<'a>,
    ) -> AsyncWritableStorage<'a>;

    #[cfg(feature = "async")]
    /// Create an asynchronous listable transformer.
    fn create_async_listable_transformer<'a>(
        &'a self,
        storage: AsyncListableStorage<'a>,
    ) -> AsyncListableStorage<'a>;

    #[cfg(feature = "async")]
    /// Create an asynchronous readable and listable transformer.
    fn create_async_readable_listable_transformer<'a>(
        &'a self,
        storage: AsyncReadableListableStorage<'a>,
    ) -> AsyncReadableListableStorage<'a>;
}
