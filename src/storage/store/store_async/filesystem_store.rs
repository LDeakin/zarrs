//! A filesystem store.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html>.

use crate::{
    object_store_impl,
    storage::{
        store_lock::{AsyncDefaultStoreLocks, AsyncStoreLocks},
        StorageError,
    },
};

use std::{path::Path, sync::Arc};

// // Register the store.
// inventory::submit! {
//     ReadableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     WritableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     ListableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     ReadableWritableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }

// #[allow(clippy::similar_names)]
// fn create_store_filesystem(uri: &str) -> Result<AsyncFilesystemStore, StorePluginCreateError> {
//     let url = url::Url::parse(uri)?;
//     let path = std::path::PathBuf::from(url.path());
//     AsyncFilesystemStore::new(path).map_err(|e| StorePluginCreateError::Other(e.to_string()))
// }

/// A file system store.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html>.
#[derive(Debug)]
pub struct AsyncFilesystemStore {
    object_store: object_store::local::LocalFileSystem,
    locks: AsyncStoreLocks,
    // sort: bool,
}

impl AsyncFilesystemStore {
    /// Create a new file system store at a given `base_path`.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if `base_directory`:
    ///   - is not valid, or
    ///   - it points to an existing file rather than a directory.
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, StorageError> {
        Self::new_with_locks(base_path, Arc::new(AsyncDefaultStoreLocks::default()))
    }

    /// Create a new file system store at a given `base_path` with non-default store locks.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if `base_directory`:
    ///   - is not valid, or
    ///   - it points to an existing file rather than a directory.
    pub fn new_with_locks<P: AsRef<Path>>(
        base_path: P,
        store_locks: AsyncStoreLocks,
    ) -> Result<Self, StorageError> {
        let base_path = base_path.as_ref().to_path_buf();
        if base_path.to_str().is_none() {
            return Err(StorageError::from(format!(
                "invalid base path {base_path:?}"
            )));
        }

        if !base_path.exists() {
            // the path does not exist, so try and create it
            std::fs::create_dir_all(&base_path).map_err(StorageError::IOError)?;
        };

        let object_store = object_store::local::LocalFileSystem::new_with_prefix(base_path)?;
        Ok(Self {
            object_store,
            locks: store_locks,
        })
    }
}

object_store_impl!(AsyncFilesystemStore, object_store, locks);

#[cfg(test)]
mod tests {
    use crate::storage::{
        AsyncListableStorageTraits, AsyncReadableStorageTraits, AsyncWritableStorageTraits,
        StoreKeyStartValue, StorePrefix,
    };

    use super::*;
    use std::error::Error;

    #[tokio::test]
    async fn filesystem_set() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let store = AsyncFilesystemStore::new(path.path())?;
        let key = "a/b".try_into()?;
        store.set(&key, &[0, 1, 2]).await?;
        assert_eq!(store.get(&key).await?.unwrap(), &[0, 1, 2]);
        store
            .set_partial_values(&[StoreKeyStartValue::new(key.clone(), 1, &[3, 4])])
            .await?;
        assert_eq!(store.get(&key).await?.unwrap(), &[0, 3, 4]);
        Ok(())
    }

    #[tokio::test]
    async fn filesystem_list() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let store = AsyncFilesystemStore::new(path.path())?;

        store.set(&"a/b".try_into()?, &[]).await?;
        store.set(&"a/c".try_into()?, &[]).await?;
        store.set(&"a/d/e".try_into()?, &[]).await?;
        store.set(&"a/d/f".try_into()?, &[]).await?;
        store.erase(&"a/d/e".try_into()?).await?;
        assert_eq!(
            store.list().await?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"a/".try_into()?).await?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"a/d/".try_into()?).await?,
            &["a/d/f".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"".try_into()?).await?,
            &["a/b".try_into()?, "a/c".try_into()?, "a/d/f".try_into()?]
        );

        // assert!(crate::storage::node_exists(&store, &"/a/b".try_into()?).await?);
        // assert!(crate::storage::node_exists_listable(&store, &"/a/b".try_into()?).await?);

        Ok(())
    }

    #[tokio::test]
    async fn filesystem_list_dir() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let store = AsyncFilesystemStore::new(path.path())?;
        store.set(&"a/b".try_into()?, &[]).await?;
        store.set(&"a/c".try_into()?, &[]).await?;
        store.set(&"a/d/e".try_into()?, &[]).await?;
        store.set(&"a/f/g".try_into()?, &[]).await?;
        store.set(&"a/f/h".try_into()?, &[]).await?;
        store.set(&"b/c/d".try_into()?, &[]).await?;

        let list_dir = store.list_dir(&StorePrefix::new("a/")?).await?;

        assert_eq!(list_dir.keys(), &["a/b".try_into()?, "a/c".try_into()?,]);
        assert_eq!(
            list_dir.prefixes(),
            &["a/d/".try_into()?, "a/f/".try_into()?,]
        );
        Ok(())
    }
}
