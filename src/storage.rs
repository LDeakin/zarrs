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
pub mod storage_transformer;
mod storage_value_io;
pub mod store;

use std::{path::PathBuf, sync::Arc};

use thiserror::Error;

use crate::{
    array::{ArrayMetadata, ChunkKeyEncoding},
    byte_range::{ByteOffset, ByteRange, InvalidByteRangeError},
    group::{GroupMetadata, GroupMetadataV3},
    node::{Node, NodeMetadata, NodeNameError, NodePath, NodePathError},
};

pub use self::store::{
    StoreKey, StoreKeyError, StoreKeys, StorePrefix, StorePrefixError, StorePrefixes,
};

pub use self::storage_transformer::StorageTransformerChain;

pub use storage_value_io::StorageValueIO;

/// [`Arc`] wrapped readable storage.
pub type ReadableStorage<'a> = Arc<dyn ReadableStorageTraits + 'a>;

/// [`Arc`] wrapped writable storage.
pub type WritableStorage<'a> = Arc<dyn WritableStorageTraits + 'a>;

/// [`Arc`] wrapped listable storage.
pub type ListableStorage<'a> = Arc<dyn ListableStorageTraits + 'a>;

/// [`Arc`] wrapped readable and writable storage.
pub type ReadableWritableStorage<'a> = Arc<dyn ReadableWritableStorageTraits + 'a>;

/// Readable storage traits.
pub trait ReadableStorageTraits: Send + Sync {
    /// Retrieve the value (bytes) associated with a given [`StoreKey`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the store key does not exist or there is an error with the underlying store.
    fn get(&self, key: &StoreKey) -> Result<Vec<u8>, StorageError>;

    /// Retrieve partial bytes from a list of [`StoreKeyRange`].
    ///
    /// # Arguments
    /// * `key_ranges`: ordered set of ([`StoreKey`], [`ByteRange`]) pairs. A key may occur multiple times with different ranges.
    ///
    /// # Output
    /// A a list of values in the order of the `key_ranges`. It will be empty for missing keys.
    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Vec<Result<Vec<u8>, StorageError>>;

    /// Return the size in bytes of the readable storage.
    fn size(&self) -> u64;

    /// Return the size in bytes of the value at `key` if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the key does not exist or there is an underlying error with the store.
    fn size_key(&self, key: &StoreKey) -> Result<u64, StorageError>;
}

/// Listable storage traits.
pub trait ListableStorageTraits: Send + Sync {
    /// Retrieve all [`StoreKeys`] in the store.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying error with the store.
    fn list(&self) -> Result<StoreKeys, StorageError>;

    /// Retrieve all [`StoreKeys`] with a given [`StorePrefix`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the prefix is not a directory or there is an underlying error with the store.
    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError>;

    /// Retrieve all [`StoreKeys`] and [`StorePrefix`] which are direct children of [`StorePrefix`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the prefix is not a directory or there is an underlying error with the store.
    ///
    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError>;
}

/// Writable storage traits.
pub trait WritableStorageTraits: Send + Sync {
    /// Store bytes at a [`StoreKey`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] on failure to store.
    fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError>;

    /// Store bytes according to a list of [`StoreKeyStartValue`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] on failure to store.
    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError>;

    /// Erase a [`StoreKey`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the store key is not in the store, or the erase otherwise fails.
    fn erase(&self, key: &StoreKey) -> Result<(), StorageError>;

    /// Erase a list of [`StoreKey`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if a store key is not in the store, or the erase otherwise fails.
    fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        for key in keys {
            self.erase(key)?;
        }
        Ok(())
    }

    /// Erase all [`StoreKey`] under [`StorePrefix`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] is the prefix is not in the store, or the erase otherwise fails.
    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError>;
}

/// A supertrait of [`ReadableStorageTraits`] and [`WritableStorageTraits`].
pub trait ReadableWritableStorageTraits: ReadableStorageTraits + WritableStorageTraits {}

/// A [`StoreKey`] and [`ByteRange`].
#[derive(Debug)]
pub struct StoreKeyRange {
    /// The key for the range.
    key: StoreKey,
    /// The byte range.
    byte_range: ByteRange,
}

impl StoreKeyRange {
    /// Create a new [`StoreKeyRange`].
    #[must_use]
    pub fn new(key: StoreKey, byte_range: ByteRange) -> StoreKeyRange {
        StoreKeyRange { key, byte_range }
    }
}

/// A [`StoreKey`], [`ByteOffset`], and value (bytes).
#[derive(Debug)]
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
    pub fn new(key: StoreKey, start: ByteOffset, value: &[u8]) -> StoreKeyStartValue {
        StoreKeyStartValue { key, start, value }
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
    pub fn keys(&self) -> &StoreKeys {
        &self.keys
    }

    /// Returns the prefixes.
    #[must_use]
    pub fn prefixes(&self) -> &StorePrefixes {
        &self.prefixes
    }
}

/// A storage error.
#[derive(Debug, Error)]
pub enum StorageError {
    /// A write operation was attempted on a read only store.
    #[error("a write operation was attempted on a read only store")]
    ReadOnly,
    /// A key was not found.
    #[error("key {0} was not found")]
    KeyNotFound(StoreKey),
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// An error serializing or deserializing JSON.
    #[error(transparent)]
    InvalidJSON(#[from] serde_json::Error),
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
    /// Any other error.
    #[error("{0}")]
    Other(String),
}

/// Return the metadata key given a node path.
#[must_use]
pub fn meta_key(path: &NodePath) -> StoreKey {
    let path = path.as_str();
    if path.eq("/") {
        unsafe { StoreKey::new_unchecked("zarr.json".to_string()) }
    } else {
        let path = path.strip_prefix('/').unwrap_or(path);
        unsafe { StoreKey::new_unchecked(path.to_string() + "/zarr.json") }
    }
}

/// Return the data key given a node path, chunk grid coordinates, and a chunk key encoding.
#[must_use]
pub fn data_key(
    path: &NodePath,
    chunk_grid_indices: &[usize],
    chunk_key_encoding: &ChunkKeyEncoding,
) -> StoreKey {
    let path = path.as_str();
    let path = path.strip_prefix('/').unwrap_or(path);
    let mut key_path = PathBuf::from(path);
    key_path.push(chunk_key_encoding.encode(chunk_grid_indices).as_str());
    unsafe { StoreKey::new_unchecked(key_path.to_string_lossy().to_string()) }
}

/// Get the child nodes.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn get_child_nodes<TStorage: ReadableStorageTraits + ListableStorageTraits>(
    storage: &TStorage,
    path: &NodePath,
) -> Result<Vec<Node>, StorageError> {
    let prefixes = discover_children(storage, path)?;
    let mut nodes: Vec<Node> = Vec::new();
    for prefix in &prefixes {
        let child_metadata_bytes = storage.get(&meta_key(&prefix.try_into()?));
        let child_metadata = match child_metadata_bytes {
            Ok(child_metadata) => {
                let metadata: NodeMetadata = serde_json::from_slice(child_metadata.as_slice())?;
                metadata
            }
            Err(_) => NodeMetadata::Group(GroupMetadataV3::default().into()),
        };
        let path: NodePath = prefix.try_into()?;
        let children = match child_metadata {
            NodeMetadata::Array(_) => Vec::default(),
            NodeMetadata::Group(_) => get_child_nodes(storage, &path)?,
        };
        nodes.push(Node::new(path, child_metadata, children));
    }
    Ok(nodes)
}

// /// Create a new [`Hierarchy`].
// ///
// /// # Errors
// ///
// /// Returns a [`StorageError`] if there is an underlying error with the store.
// pub fn create_hierarchy<TStorage: ReadableStorageTraits + ListableStorageTraits>(
//     storage: &TStorage,
// ) -> Result<Hierarchy, StorageError> {
//     let root_path: NodePath = NodePath::new("/")?;
//     let root_metadata = storage.get(&meta_key(&root_path));
//     let root_metadata: NodeMetadata = match root_metadata {
//         Ok(root_metadata) => serde_json::from_slice(root_metadata.as_slice())?,
//         Err(..) => NodeMetadata::Group(GroupMetadata::default()), // root metadata does not exist, assume implicit group
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

/// Create a group.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn create_group(
    storage: &dyn WritableStorageTraits,
    path: &NodePath,
    group: &GroupMetadata,
) -> Result<(), StorageError> {
    let json = serde_json::to_vec_pretty(group)?;
    storage.set(&meta_key(path), &json)?;
    Ok(())
}

/// Create an array.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn create_array(
    storage: &dyn WritableStorageTraits,
    path: &NodePath,
    array: &ArrayMetadata,
) -> Result<(), StorageError> {
    let json = serde_json::to_vec_pretty(array)?;
    storage.set(&meta_key(path), &json)?;
    Ok(())
}

/// Store a chunk.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn store_chunk(
    storage: &dyn WritableStorageTraits,
    array_path: &NodePath,
    chunk_grid_indices: &[usize],
    chunk_key_encoding: &ChunkKeyEncoding,
    chunk_serialised: &[u8],
) -> Result<(), StorageError> {
    storage.set(
        &data_key(array_path, chunk_grid_indices, chunk_key_encoding),
        chunk_serialised,
    )?;
    Ok(())
}

/// Retrieve a chunk.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn retrieve_chunk(
    storage: &dyn ReadableStorageTraits,
    array_path: &NodePath,
    chunk_grid_indices: &[usize],
    chunk_key_encoding: &ChunkKeyEncoding,
) -> Result<Vec<u8>, StorageError> {
    storage.get(&data_key(
        array_path,
        chunk_grid_indices,
        chunk_key_encoding,
    ))
}

/// Retrieve byte ranges from a chunk.
pub fn retrieve_partial_values(
    storage: &dyn ReadableStorageTraits,
    array_path: &NodePath,
    chunk_grid_indices: &[usize],
    chunk_key_encoding: &ChunkKeyEncoding,
    bytes_ranges: &[ByteRange],
) -> Vec<Result<Vec<u8>, StorageError>> {
    let key = data_key(array_path, chunk_grid_indices, chunk_key_encoding);
    let key_ranges: Vec<StoreKeyRange> = bytes_ranges
        .iter()
        .map(|byte_range| StoreKeyRange::new(key.clone(), *byte_range))
        .collect();
    storage.get_partial_values(&key_ranges)
}

/// Discover the children of a node.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn discover_children<TStorage: ReadableStorageTraits + ListableStorageTraits>(
    storage: &TStorage,
    path: &NodePath,
) -> Result<StorePrefixes, StorageError> {
    let prefix: StorePrefix = path.try_into()?;
    let children: Result<Vec<_>, _> = storage
        .list_dir(&prefix)?
        .prefixes()
        .iter()
        .filter(|v| !v.as_str().starts_with("__"))
        .map(|v| StorePrefix::new(v.as_str()))
        .collect();
    Ok(children?)
}

/// Discover all nodes.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
///
pub fn discover_nodes(storage: &dyn ListableStorageTraits) -> Result<StoreKeys, StorageError> {
    storage.list_prefix(&"/".try_into()?)
}

/// Erase a node.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn erase_node(
    storage: &dyn WritableStorageTraits,
    path: &NodePath,
) -> Result<(), StorageError> {
    let prefix = path.try_into()?;
    storage.erase_prefix(&prefix)
}

/// Check if a node exists.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn node_exists<TStorage: ReadableStorageTraits + ListableStorageTraits>(
    storage: &TStorage,
    path: &NodePath,
) -> Result<bool, StorageError> {
    Ok(storage
        .get(&meta_key(path))
        .map_or(storage.list_dir(&path.try_into()?).is_ok(), |_| true))
}

/// Check if a node exists.
///
/// # Errors
///
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub fn node_exists_listable<TStorage: ListableStorageTraits>(
    storage: &TStorage,
    path: &NodePath,
) -> Result<bool, StorageError> {
    let prefix: StorePrefix = path.try_into()?;
    if let Some(parent) = prefix.parent() {
        storage.list_dir(&parent).map(|keys_prefixes| {
            !keys_prefixes.keys().is_empty() || !keys_prefixes.prefixes().is_empty()
        })
    } else {
        Ok(false)
    }
}

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
        //     "mt_log: ",
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
        let transformer_listable =
            storage_transformer_chain.create_listable_transformer(store.clone());

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
