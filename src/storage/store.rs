//! Zarr stores. Includes [filesystem](FilesystemStore) and [memory](MemoryStore) implementations.
//!
//! All stores must be Send + Sync with internally managed synchronisation.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id21>

mod filesystem;
mod memory;
// mod store_plugin;

pub use filesystem::{FilesystemStore, FilesystemStoreCreateError};
pub use memory::MemoryStore;

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
pub use http::{HTTPStore, HTTPStoreCreateError};

// pub use store_plugin::{StorePlugin, StorePluginCreateError}; // Currently disabled.

use std::sync::Arc;

/// An [`Arc`] wrapped readable store.
pub type ReadableStore = Arc<dyn super::ReadableStorageTraits>;

/// An [`Arc`] wrapped writable store.
pub type WritableStore = Arc<dyn super::WritableStorageTraits>;

/// An [`Arc`] wrapped listable store.
pub type ListableStore = Arc<dyn super::ListableStorageTraits>;

// /// A readable store plugin.
// pub type ReadableStorePlugin = StorePlugin<ReadableStore>;
// inventory::collect!(ReadableStorePlugin);

// /// A writable store plugin.
// pub type WritableStorePlugin = StorePlugin<WritableStore>;
// inventory::collect!(WritableStorePlugin);

// /// A listable store plugin.
// pub type ListableStorePlugin = StorePlugin<ListableStore>;
// inventory::collect!(ListableStorePlugin);

// /// A readable and writable store plugin.
// pub type ReadableWritableStorePlugin = StorePlugin<ReadableWritableStore>;
// inventory::collect!(ReadableWritableStorePlugin);

// /// Traits for a store extension.
// pub trait StoreExtension: Send + Sync {
//     // /// The URI scheme of the store, if it has one.
//     // fn uri_scheme(&self) -> Option<&'static str>;
// }

// /// Get a readable store from a Uniform Resource Identifier (URI).
// ///
// /// # Errors
// ///
// /// Returns a [`StorePluginCreateError`] if:
// ///  - the URI could not be parsed,
// ///  - a store is note registered for the URI scheme, or
// ///  - there was a failure creating the store.
// #[allow(clippy::similar_names)]
// pub fn readable_store_from_uri(
//     uri: &str,
// ) -> std::result::Result<ReadableStore, StorePluginCreateError> {
//     let url = url::Url::parse(uri)?;
//     let scheme = url.scheme();

//     for plugin in inventory::iter::<ReadableStorePlugin> {
//         if plugin.uri_scheme() == scheme {
//             return plugin.create(uri);
//         }
//     }

//     Err(StorePluginCreateError::UnsupportedScheme(
//         scheme.to_string(),
//     ))
// }

// /// Get a writable store from a Uniform Resource Identifier (URI).
// ///
// /// # Errors
// ///
// /// Returns a [`StorePluginCreateError`] if:
// ///  - the URI could not be parsed,
// ///  - a store is note registered for the URI scheme, or
// ///  - there was a failure creating the store.
// #[allow(clippy::similar_names)]
// pub fn writable_store_from_uri(
//     uri: &str,
// ) -> std::result::Result<WritableStore, StorePluginCreateError> {
//     let url = url::Url::parse(uri)?;
//     let scheme = url.scheme();

//     for plugin in inventory::iter::<WritableStorePlugin> {
//         if plugin.uri_scheme() == scheme {
//             return plugin.create(uri);
//         }
//     }

//     Err(StorePluginCreateError::UnsupportedScheme(
//         scheme.to_string(),
//     ))
// }

// /// Get a listable store from a Uniform Resource Identifier (URI).
// ///
// /// # Errors
// ///
// /// Returns a [`StorePluginCreateError`] if:
// ///  - the URI could not be parsed,
// ///  - a store is note registered for the URI scheme, or
// ///  - there was a failure creating the store.
// #[allow(clippy::similar_names)]
// pub fn listable_store_from_uri(
//     uri: &str,
// ) -> std::result::Result<ListableStore, StorePluginCreateError> {
//     let url = url::Url::parse(uri)?;
//     let scheme = url.scheme();

//     for plugin in inventory::iter::<ListableStorePlugin> {
//         if plugin.uri_scheme() == scheme {
//             return plugin.create(uri);
//         }
//     }

//     Err(StorePluginCreateError::UnsupportedScheme(
//         scheme.to_string(),
//     ))
// }

// /// Get a readable and writable store from a Uniform Resource Identifier (URI).
// ///
// /// # Errors
// ///
// /// Returns a [`StorePluginCreateError`] if:
// ///  - the URI could not be parsed,
// ///  - a store is note registered for the URI scheme, or
// ///  - there was a failure creating the store.
// #[allow(clippy::similar_names)]
// pub fn readable_writable_store_from_uri(
//     uri: &str,
// ) -> std::result::Result<ReadableWritableStore, StorePluginCreateError> {
//     let url = url::Url::parse(uri)?;
//     let scheme = url.scheme();

//     for plugin in inventory::iter::<ReadableWritableStorePlugin> {
//         if plugin.uri_scheme() == scheme {
//             return plugin.create(uri);
//         }
//     }

//     Err(StorePluginCreateError::UnsupportedScheme(
//         scheme.to_string(),
//     ))
// }
