//! Zarr storage ([stores](store) and [storage transformers](storage_transformer)).
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#storage>.
//!
//! A Zarr [store] is a system that can be used to store and retrieve data from a Zarr hierarchy.
//! For example: a filesystem, HTTP server, FTP server, Amazon S3 bucket, ZIP file, etc.
//!
//! A Zarr [storage transformer](storage_transformer) modifies a request to read or write data before passing that request to a following storage transformer or store.
//! A [`StorageTransformerChain`] represents a sequence of storage transformers.
//! A storage transformer chain and individual storage transformers all have the same interface as a [store].
//!
//! This module defines abstract store interfaces, includes various store and storage transformers, and has functions for performing the store operations defined at <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#operations>.

pub mod storage_adapter;
mod storage_handle;
mod storage_sync;
pub mod storage_transformer;
mod storage_value_io;
pub mod store;
mod store_key;
// pub mod store_lock;
mod store_prefix;

#[cfg(feature = "async")]
mod storage_async;

use std::{path::PathBuf, sync::Arc};

use thiserror::Error;

use crate::{
    array::ChunkKeyEncoding,
    byte_range::{ByteOffset, ByteRange, InvalidByteRangeError},
    node::{NodeNameError, NodePath, NodePathError},
};

pub use store_key::{StoreKey, StoreKeyError, StoreKeys};
pub use store_prefix::{StorePrefix, StorePrefixError, StorePrefixes};

#[cfg(feature = "async")]
pub use self::storage_async::{
    async_create_array, async_create_group, async_discover_children, async_discover_nodes,
    async_erase_chunk, async_erase_metadata, async_erase_node, async_get_child_nodes,
    async_node_exists, async_node_exists_listable, async_retrieve_chunk,
    async_retrieve_partial_values, async_store_chunk, async_store_set_partial_values,
    AsyncListableStorageTraits, AsyncReadableListableStorageTraits, AsyncReadableStorageTraits,
    AsyncReadableWritableListableStorageTraits, AsyncReadableWritableStorageTraits,
    AsyncWritableStorageTraits,
};

pub use self::storage_sync::{
    create_array, create_group, discover_children, discover_nodes, erase_chunk, erase_metadata,
    erase_node, get_child_nodes, node_exists, node_exists_listable, retrieve_chunk,
    retrieve_partial_values, store_chunk, store_set_partial_values, ListableStorageTraits,
    ReadableListableStorageTraits, ReadableStorageTraits, ReadableWritableListableStorageTraits,
    ReadableWritableStorageTraits, WritableStorageTraits,
};
pub use self::storage_transformer::StorageTransformerChain;

pub use self::storage_handle::StorageHandle;

pub use storage_value_io::StorageValueIO;

/// [`Arc`] wrapped readable storage.
pub type ReadableStorage = Arc<dyn ReadableStorageTraits>;

/// [`Arc`] wrapped writable storage.
pub type WritableStorage = Arc<dyn WritableStorageTraits>;

/// [`Arc`] wrapped readable and writable storage.
pub type ReadableWritableStorage = Arc<dyn ReadableWritableStorageTraits>;

/// [`Arc`] wrapped readable, writable, and listable storage.
pub type ReadableWritableListableStorage = Arc<dyn ReadableWritableListableStorageTraits>;

/// [`Arc`] wrapped listable storage.
pub type ListableStorage = Arc<dyn ListableStorageTraits>;

/// [`Arc`] wrapped readable and listable storage.
pub type ReadableListableStorage = Arc<dyn ReadableListableStorageTraits>;

#[cfg(feature = "async")]
/// [`Arc`] wrapped asynchronous readable storage.
pub type AsyncReadableStorage = Arc<dyn AsyncReadableStorageTraits>;

#[cfg(feature = "async")]
/// [`Arc`] wrapped asynchronous writable storage.
pub type AsyncWritableStorage = Arc<dyn AsyncWritableStorageTraits>;

#[cfg(feature = "async")]
/// [`Arc`] wrapped asynchronous listable storage.
pub type AsyncListableStorage = Arc<dyn AsyncListableStorageTraits>;

#[cfg(feature = "async")]
/// [`Arc`] wrapped asynchronous readable and listable storage.
pub type AsyncReadableListableStorage = Arc<dyn AsyncReadableListableStorageTraits>;

#[cfg(feature = "async")]
/// [`Arc`] wrapped asynchronous readable, writable and listable storage.
pub type AsyncReadableWritableListableStorage = Arc<dyn AsyncReadableWritableListableStorageTraits>;

/// A [`StoreKey`] and [`ByteRange`].
#[derive(Debug, Clone)]
pub struct StoreKeyRange {
    /// The key for the range.
    key: StoreKey,
    /// The byte range.
    byte_range: ByteRange,
}

impl StoreKeyRange {
    /// Create a new [`StoreKeyRange`].
    #[must_use]
    pub const fn new(key: StoreKey, byte_range: ByteRange) -> Self {
        Self { key, byte_range }
    }
}

impl std::fmt::Display for StoreKeyRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}:{}", self.key, self.byte_range)
    }
}

/// A [`StoreKey`], [`ByteOffset`], and value (bytes).
#[derive(Debug, Clone)]
#[must_use]
pub struct StoreKeyStartValue<'a> {
    /// The key.
    key: StoreKey,
    /// The starting byte offset.
    start: ByteOffset,
    /// The store value.
    value: &'a [u8],
}

impl StoreKeyStartValue<'_> {
    /// Create a new [`StoreKeyStartValue`].
    pub const fn new(key: StoreKey, start: ByteOffset, value: &[u8]) -> StoreKeyStartValue {
        StoreKeyStartValue { key, start, value }
    }

    /// Get the offset of exclusive end of the [`StoreKeyStartValue`].
    #[must_use]
    pub const fn end(&self) -> ByteOffset {
        self.start + self.value.len() as u64
    }
}

/// [`StoreKeys`] and [`StorePrefixes`].
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
#[allow(dead_code)]
pub struct StoreKeysPrefixes {
    keys: StoreKeys,
    prefixes: StorePrefixes,
}

impl StoreKeysPrefixes {
    /// Returns the keys.
    #[must_use]
    pub const fn keys(&self) -> &StoreKeys {
        &self.keys
    }

    /// Returns the prefixes.
    #[must_use]
    pub const fn prefixes(&self) -> &StorePrefixes {
        &self.prefixes
    }
}

/// A storage error.
#[derive(Debug, Error)]
pub enum StorageError {
    /// A write operation was attempted on a read only store.
    #[error("a write operation was attempted on a read only store")]
    ReadOnly,
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    // /// An error serialising or deserialising JSON.
    // #[error(transparent)]
    // InvalidJSON(#[from] serde_json::Error),
    /// An error parsing the metadata for a key.
    #[error("error parsing metadata for {0}: {1}")]
    InvalidMetadata(StoreKey, String),
    /// An invalid store prefix.
    #[error("invalid store prefix {0}")]
    StorePrefixError(#[from] StorePrefixError),
    /// An invalid store key.
    #[error("invalid store key {0}")]
    InvalidStoreKey(#[from] StoreKeyError),
    /// An invalid node path.
    #[error("invalid node path {0}")]
    NodePathError(#[from] NodePathError),
    /// An invalid node name.
    #[error("invalid node name {0}")]
    NodeNameError(#[from] NodeNameError),
    /// An invalid byte range.
    #[error("invalid byte range {0}")]
    InvalidByteRangeError(#[from] InvalidByteRangeError),
    /// The requested method is not supported.
    #[error("{0}")]
    Unsupported(String),
    /// Unknown key size where the key size must be known.
    #[error("{0}")]
    UnknownKeySize(StoreKey),
    /// Any other error.
    #[error("{0}")]
    Other(String),
}

impl From<&str> for StorageError {
    fn from(err: &str) -> Self {
        Self::Other(err.to_string())
    }
}

impl From<String> for StorageError {
    fn from(err: String) -> Self {
        Self::Other(err)
    }
}

#[cfg(feature = "opendal")]
impl From<opendal::Error> for StorageError {
    fn from(err: opendal::Error) -> Self {
        Self::Other(err.to_string())
    }
}

/// Return the metadata key given a node path for a specified metadata file name (e.g. zarr.json, .zarray, .zgroup, .zaatrs).
#[must_use]
fn meta_key_any(path: &NodePath, metadata_file_name: &str) -> StoreKey {
    let path = path.as_str();
    if path.eq("/") {
        unsafe { StoreKey::new_unchecked(metadata_file_name.to_string()) }
    } else {
        let path = path.strip_prefix('/').unwrap_or(path);
        unsafe { StoreKey::new_unchecked(format!("{path}/{metadata_file_name}")) }
    }
}

/// Return the Zarr V3 metadata key (zarr.json) given a node path.
#[must_use]
pub fn meta_key(path: &NodePath) -> StoreKey {
    meta_key_any(path, "zarr.json")
}

/// Return the Zarr V2 array metadata key (.zarray) given a node path.
#[must_use]
pub fn meta_key_v2_array(path: &NodePath) -> StoreKey {
    meta_key_any(path, ".zarray")
}

/// Return the Zarr V2 group metadata key (.zgroup) given a node path.
#[must_use]
pub fn meta_key_v2_group(path: &NodePath) -> StoreKey {
    meta_key_any(path, ".zgroup")
}

/// Return the Zarr V2 user-defined attributes key (.zattrs) given a node path.
#[must_use]
pub fn meta_key_v2_attributes(path: &NodePath) -> StoreKey {
    meta_key_any(path, ".zattrs")
}

/// Return the data key given a node path, chunk grid coordinates, and a chunk key encoding.
#[must_use]
pub fn data_key(
    path: &NodePath,
    chunk_grid_indices: &[u64],
    chunk_key_encoding: &ChunkKeyEncoding,
) -> StoreKey {
    let path = path.as_str();
    let path = path.strip_prefix('/').unwrap_or(path);
    let mut key_path = PathBuf::from(path);
    key_path.push(chunk_key_encoding.encode(chunk_grid_indices).as_str());
    unsafe { StoreKey::new_unchecked(key_path.to_string_lossy().to_string()) }
}

// /// Create a new [`Hierarchy`].
// ///
// /// # Errors
// /// Returns a [`StorageError`] if there is an underlying error with the store.
// pub fn create_hierarchy<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
//     storage: &TStorage,
// ) -> Result<Hierarchy, StorageError> {
//     let root_path: NodePath = NodePath::new("/")?;
//     let root_metadata = storage.get(&meta_key(&root_path));
//     let root_metadata: NodeMetadata = match root_metadata {
//         Ok(root_metadata) => serde_json::from_slice(root_metadata.as_slice())?,
//         Err(..) => NodeMetadata::Group(GroupMetadataV3::default()), // root metadata does not exist, assume implicit group
//     };

//     let children = get_child_nodes(storage, &root_path)?;
//     let root_node = Node {
//         name: NodeName::root(),
//         path: root_path,
//         children,
//         metadata: root_metadata,
//     };
//     Ok(Hierarchy { root: root_node })
// }

#[cfg(test)]
mod tests {
    use std::io::Write;

    use self::store::MemoryStore;

    use super::*;

    #[test]
    fn transformers_multithreaded() {
        use rayon::prelude::*;

        let store = Arc::new(MemoryStore::default());

        let log_writer = Arc::new(std::sync::Mutex::new(std::io::BufWriter::new(
            std::io::stdout(),
        )));

        // let storage_transformer_usage_log = Arc::new(self::storage_transformer::UsageLogStorageTransformer::new(
        //     || "mt_log: ".to_string(),
        //     log_writer.clone(),
        // ));
        let storage_transformer_performance_metrics =
            Arc::new(self::storage_transformer::PerformanceMetricsStorageTransformer::new());
        let storage_transformer_chain = StorageTransformerChain::new(vec![
            // storage_transformer_usage_log.clone(),
            storage_transformer_performance_metrics.clone(),
        ]);
        let transformer =
            storage_transformer_chain.create_readable_writable_transformer(store.clone());
        let transformer_listable = storage_transformer_chain.create_listable_transformer(store);

        (0..10).into_par_iter().for_each(|_| {
            transformer_listable.list().unwrap();
        });

        (0..10).into_par_iter().for_each(|i| {
            transformer
                .set(&StoreKey::new(&i.to_string()).unwrap(), &[i; 5])
                .unwrap();
        });

        for i in 0..10 {
            let _ = transformer.get(&StoreKey::new(&i.to_string()).unwrap());
        }

        log_writer.lock().unwrap().flush().unwrap();

        println!(
            "stats\n\t{}\n\t{}\n\t{}\n\t{}",
            storage_transformer_performance_metrics.bytes_written(),
            storage_transformer_performance_metrics.bytes_read(),
            storage_transformer_performance_metrics.writes(),
            storage_transformer_performance_metrics.reads()
        );
    }
}
