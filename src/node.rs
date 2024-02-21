//! Zarr nodes.
//!
//! A node in a Zarr hierarchy represents either an [`Array`](crate::array::Array) or a [`Group`](crate::group::Group).
//!
//! A [`Node`] has an associated [`NodePath`], [`NodeMetadata`], and children.
//!
//! The [`Node::hierarchy_tree`] function can be used to create a string representation of a the hierarchy below a node.

mod node_metadata;
mod node_name;
mod node_path;

pub use node_metadata::NodeMetadata;
pub use node_name::{NodeName, NodeNameError};
pub use node_path::{NodePath, NodePathError};
use thiserror::Error;

use crate::{
    array::ArrayMetadata,
    group::GroupMetadataV3,
    storage::{
        get_child_nodes, meta_key, ListableStorageTraits, ReadableStorageTraits, StorageError,
    },
};

#[cfg(feature = "async")]
use crate::storage::{
    async_get_child_nodes, AsyncListableStorageTraits, AsyncReadableStorageTraits,
};

/// A Zarr hierarchy node.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#hierarchy>.
#[derive(Debug, Clone)]
pub struct Node {
    /// Node path.
    path: NodePath,
    /// Node metadata.
    metadata: NodeMetadata,
    /// Node children.
    ///
    /// Only group nodes can have children.
    children: Vec<Node>,
}

/// A node creation error.
#[derive(Debug, Error)]
pub enum NodeCreateError {
    /// An invalid node path
    #[error(transparent)]
    NodePathError(#[from] NodePathError),
    /// A storage error.
    #[error(transparent)]
    StorageError(#[from] StorageError),
}

impl Node {
    /// Create a new node at `path` and read metadata and children from `storage`.
    ///
    /// # Errors
    ///
    /// Returns [`NodeCreateError`] if metadata is invalid or there is a failure to list child nodes.
    pub fn new<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits>(
        storage: &TStorage,
        path: &str,
    ) -> Result<Self, NodeCreateError> {
        let path: NodePath = path.try_into()?;
        let key = meta_key(&path);
        let metadata = storage.get(&key)?;
        let metadata: NodeMetadata = match metadata {
            Some(metadata) => serde_json::from_slice(metadata.as_slice()).map_err(|e| {
                NodeCreateError::StorageError(StorageError::InvalidMetadata(key, e.to_string()))
            })?,
            None => NodeMetadata::Group(GroupMetadataV3::default().into()),
        };
        let children = match metadata {
            NodeMetadata::Array(_) => Vec::default(),
            NodeMetadata::Group(_) => get_child_nodes(storage, &path)?,
        };
        let node = Self {
            path,
            metadata,
            children,
        };
        Ok(node)
    }

    #[cfg(feature = "async")]
    /// Asynchronously create a new node at `path` and read metadata and children from `storage`.
    ///
    /// # Errors
    ///
    /// Returns [`NodeCreateError`] if metadata is invalid or there is a failure to list child nodes.
    pub async fn async_new<
        TStorage: ?Sized + AsyncReadableStorageTraits + AsyncListableStorageTraits,
    >(
        storage: &TStorage,
        path: &str,
    ) -> Result<Self, NodeCreateError> {
        let path: NodePath = path.try_into()?;
        let key = meta_key(&path);
        let metadata = storage.get(&key).await?;
        let metadata: NodeMetadata = match metadata {
            Some(metadata) => serde_json::from_slice(metadata.as_slice()).map_err(|e| {
                NodeCreateError::StorageError(StorageError::InvalidMetadata(key, e.to_string()))
            })?,
            None => NodeMetadata::Group(GroupMetadataV3::default().into()),
        };
        let children = match metadata {
            NodeMetadata::Array(_) => Vec::default(),
            NodeMetadata::Group(_) => async_get_child_nodes(storage, &path).await?,
        };
        let node = Self {
            path,
            metadata,
            children,
        };
        Ok(node)
    }

    /// Create a new node at `path` with `metadata` and `children`.
    #[must_use]
    pub fn new_with_metadata(path: NodePath, metadata: NodeMetadata, children: Vec<Self>) -> Self {
        Self {
            path,
            metadata,
            children,
        }
    }

    /// Indicates if a node is the root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.path.as_str().eq("/")
    }

    /// Returns the name of the node.
    #[must_use]
    pub fn name(&self) -> NodeName {
        let name = self.path.as_str().split('/').last().unwrap_or_default();
        unsafe { NodeName::new_unchecked(name) }
    }

    /// Returns a reference to the path of the node.
    #[must_use]
    pub fn path(&self) -> &NodePath {
        &self.path
    }

    /// Returns a reference to the metadata of the node.
    #[must_use]
    pub fn metadata(&self) -> &NodeMetadata {
        &self.metadata
    }

    /// Returns a reference to the children of the node.
    #[must_use]
    pub fn children(&self) -> &[Node] {
        &self.children
    }

    /// Return a tree representation of a hierarchy as a string.
    ///
    /// Arrays are annotated with their shape and data type.
    /// For example:
    /// ```text
    /// a
    ///   baz [10000, 1000] float64
    ///   foo [10000, 1000] float64
    /// b
    /// ```
    #[must_use]
    pub fn hierarchy_tree(&self) -> String {
        fn print_metadata(name: &str, string: &mut String, metadata: &NodeMetadata) {
            match metadata {
                NodeMetadata::Array(array_metadata) => {
                    let ArrayMetadata::V3(array_metadata) = array_metadata;
                    let s = format!(
                        "{} {:?} {}",
                        name, array_metadata.shape, array_metadata.data_type
                    );
                    string.push_str(&s);
                }
                NodeMetadata::Group(_) => {
                    string.push_str(name);
                }
            };
            string.push('\n');
        }

        fn update_tree(string: &mut String, children: &[Node], depth: usize) {
            for child in children {
                let name = child.name();
                string.push_str(&" ".repeat(depth * 2));
                print_metadata(name.as_str(), string, &child.metadata);
                update_tree(string, &child.children, depth + 1);
            }
        }

        let mut string = String::default();
        print_metadata("/", &mut string, &self.metadata);
        update_tree(&mut string, &self.children, 1);
        string
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{ArrayBuilder, FillValue},
        group::GroupMetadata,
        storage::{store::MemoryStore, StoreKey, WritableStorageTraits},
    };

    use super::*;

    #[test]
    fn node_metadata_array() {
        const JSON_ARRAY: &str = r#"{
            "zarr_format": 3,
            "node_type": "array",
            "shape": [
              10000,
              1000
            ],
            "data_type": "float64",
            "chunk_grid": {
              "name": "regular",
              "configuration": {
                "chunk_shape": [
                  1000,
                  100
                ]
              }
            },
            "chunk_key_encoding": {
              "name": "default",
              "configuration": {
                "separator": "/"
              }
            },
            "fill_value": "NaN",
            "codecs": [
              {
                "name": "bytes",
                "configuration": {
                  "endian": "little"
                }
              },
              {
                "name": "gzip",
                "configuration": {
                  "level": 1
                }
              }
            ],
            "attributes": {
              "foo": 42,
              "bar": "apples",
              "baz": [
                1,
                2,
                3,
                4
              ]
            },
            "dimension_names": [
              "rows",
              "columns"
            ]
          }"#;
        serde_json::from_str::<NodeMetadata>(JSON_ARRAY).unwrap();
    }

    #[test]
    fn node_metadata_group() {
        const JSON_GROUP: &str = r#"{
        "zarr_format": 3,
        "node_type": "group",
        "attributes": {
            "spam": "ham",
            "eggs": 42
        }
    }"#;
        serde_json::from_str::<NodeMetadata>(JSON_GROUP).unwrap();
    }

    #[test]
    fn node_default() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let node_path = "/node";
        let node = Node::new(&*store, node_path).unwrap();
        assert_eq!(
            node.metadata,
            NodeMetadata::Group(GroupMetadata::V3(GroupMetadataV3::default()))
        );
    }

    #[test]
    fn node_array() {
        let store = std::sync::Arc::new(MemoryStore::new());
        let node_path = "/node";
        let array = ArrayBuilder::new(
            vec![1, 2, 3],
            crate::array::DataType::Float32,
            vec![1, 1, 1].try_into().unwrap(),
            FillValue::from(0.0f32),
        )
        .build(store.clone(), node_path)
        .unwrap();
        array.store_metadata().unwrap();
        let node = Node::new(&*store, node_path).unwrap();
        assert_eq!(node.metadata, NodeMetadata::Array(array.metadata()));
    }

    #[test]
    fn node_invalid_path() {
        let store: std::sync::Arc<MemoryStore> = std::sync::Arc::new(MemoryStore::new());
        let invalid_node_path = "node";
        assert_eq!(
            Node::new(&*store, invalid_node_path)
                .unwrap_err()
                .to_string(),
            "invalid node path node"
        );
    }

    #[test]
    fn node_invalid_metadata() {
        let store: std::sync::Arc<MemoryStore> = std::sync::Arc::new(MemoryStore::new());
        store
            .set(&StoreKey::new("node/zarr.json").unwrap(), &[0])
            .unwrap();
        assert_eq!(
            Node::new(&*store, "/node").unwrap_err().to_string(),
            "error parsing metadata for node/zarr.json: expected value at line 1 column 1"
        );
    }

    #[test]
    fn node_invalid_child() {
        let store: std::sync::Arc<MemoryStore> = std::sync::Arc::new(MemoryStore::new());
        store
            .set(&StoreKey::new("node/array/zarr.json").unwrap(), &[0])
            .unwrap();
        assert_eq!(
            Node::new(&*store, "/node").unwrap_err().to_string(),
            "error parsing metadata for node/array/zarr.json: expected value at line 1 column 1"
        );
    }

    #[test]
    fn node_root() {
        let node = Node::new_with_metadata(
            NodePath::root(),
            NodeMetadata::Group(GroupMetadata::V3(GroupMetadataV3::default())),
            vec![],
        );
        assert!(node.is_root());
    }
}
