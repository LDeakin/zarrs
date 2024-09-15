//! Zarr stores.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id21>

mod store_sync;

pub use store_sync::filesystem_store::{
    FilesystemStore, FilesystemStoreCreateError, FilesystemStoreOptions,
};
pub use store_sync::memory_store::MemoryStore;

#[cfg(feature = "http")]
#[allow(deprecated)]
pub use store_sync::http_store::{HTTPStore, HTTPStoreCreateError};
