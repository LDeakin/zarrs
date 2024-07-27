use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;

use crate::{
    byte_range::ByteRange,
    storage::{
        AsyncBytes, AsyncListableStorageTraits, AsyncReadableStorageTraits,
        AsyncReadableWritableStorageTraits, AsyncWritableStorageTraits, MaybeAsyncBytes,
        StorageError, StoreKey, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix,
    },
};

impl From<object_store::Error> for StorageError {
    fn from(err: object_store::Error) -> Self {
        Self::Other(err.to_string())
    }
}

/// Maps a [`StoreKey`] to an [`object_store`] path.
fn key_to_path(key: &StoreKey) -> object_store::path::Path {
    object_store::path::Path::from(key.as_str())
}

/// Map [`object_store::Error::NotFound`] to None, pass through other errors
fn handle_result<T>(result: Result<T, object_store::Error>) -> Result<Option<T>, StorageError> {
    match result {
        Ok(result) => Ok(Some(result)),
        Err(err) => {
            if matches!(err, object_store::Error::NotFound { .. }) {
                Ok(None)
            } else {
                Err(err.into())
            }
        }
    }
}

/// An asynchronous store backed by an [`object_store::ObjectStore`].
pub struct AsyncObjectStore<T> {
    object_store: T,
    // locks: AsyncStoreLocks,
}

impl<T: object_store::ObjectStore> AsyncObjectStore<T> {
    /// Create a new [`AsyncObjectStore`].
    #[must_use]
    pub fn new(object_store: T) -> Self {
        Self { object_store }
        // Self::new_with_locks(object_store, Arc::new(AsyncDefaultStoreLocks::default()))
    }

    // /// Create a new [`AsyncObjectStore`] with non-default store locks.
    // #[must_use]
    // pub fn new_with_locks(object_store: T, store_locks: AsyncStoreLocks) -> Self {
    //     Self {
    //         object_store,
    //         locks: store_locks,
    //     }
    // }
}

#[async_trait::async_trait]
impl<T: object_store::ObjectStore> AsyncReadableStorageTraits for AsyncObjectStore<T> {
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        let get = handle_result(self.object_store.get(&key_to_path(key)).await)?;
        if let Some(get) = get {
            let bytes = get.bytes().await?;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<AsyncBytes>>, StorageError> {
        let Some(size) = self.size_key(key).await? else {
            return Ok(None);
        };
        let ranges = byte_ranges
            .iter()
            .map(|byte_range| byte_range.to_range_usize(size))
            .collect::<Vec<_>>();
        let get_ranges = self
            .object_store
            .get_ranges(&key_to_path(key), &ranges)
            .await;
        match get_ranges {
            Ok(get_ranges) => Ok(Some(
                std::iter::zip(ranges, get_ranges)
                    .map(|(range, bytes)| {
                        if range.len() == bytes.len() {
                            Ok(bytes)
                        } else {
                            Err(StorageError::Other(format!(
                                "Unexpected length of bytes returned, expected {}, got {}",
                                range.len(),
                                bytes.len()
                            )))
                        }
                    })
                    .collect::<Result<_, StorageError>>()?,
            )),
            Err(err) => {
                if matches!(err, object_store::Error::NotFound { .. }) {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        Ok(
            handle_result(self.object_store.head(&key_to_path(key)).await)?
                .map(|meta| meta.size as u64),
        )
    }
}

#[async_trait::async_trait]
impl<T: object_store::ObjectStore> AsyncWritableStorageTraits for AsyncObjectStore<T> {
    async fn set(&self, key: &StoreKey, value: AsyncBytes) -> Result<(), StorageError> {
        self.object_store
            .put(&key_to_path(key), value.into())
            .await?;
        Ok(())
    }

    async fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        crate::storage::async_store_set_partial_values(self, key_start_values).await
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        handle_result(self.object_store.delete(&key_to_path(key)).await)?;
        Ok(())
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        let prefix: object_store::path::Path = prefix.as_str().into();
        let locations = self
            .object_store
            .list(Some(&prefix))
            .map_ok(|m| m.location)
            .boxed();
        self.object_store
            .delete_stream(locations)
            .try_collect::<Vec<Path>>()
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: object_store::ObjectStore> AsyncReadableWritableStorageTraits for AsyncObjectStore<T> {
    // async fn mutex(&self, key: &StoreKey) -> Result<AsyncStoreKeyMutex, StorageError> {
    //     Ok(self.locks.mutex(key).await)
    // }
}

#[async_trait::async_trait]
impl<T: object_store::ObjectStore> AsyncListableStorageTraits for AsyncObjectStore<T> {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        let mut list = self
            .object_store
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|object_meta| {
                object_meta.map(|object_meta| {
                    let path: &str = object_meta.location.as_ref();
                    StoreKey::try_from(path).unwrap() // FIXME
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        list.sort();
        Ok(list)
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        // TODO: Check if this is outputting everything under prefix, or just one level under
        let path: object_store::path::Path = prefix.as_str().into();
        let mut list = self
            .object_store
            .list(Some(&path))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|object_meta| {
                object_meta.map(|object_meta| {
                    let path: &str = object_meta.location.as_ref();
                    StoreKey::try_from(path).unwrap() // FIXME
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        list.sort();
        Ok(list)
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let path: object_store::path::Path = prefix.as_str().into();
        let list_result = self.object_store.list_with_delimiter(Some(&path)).await?;
        let mut prefixes = list_result
            .common_prefixes
            .iter()
            .map(|path| {
                let path: &str = path.as_ref();
                StorePrefix::new(path.to_string() + "/")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut keys = list_result
            .objects
            .iter()
            .map(|object_meta| {
                let path: &str = object_meta.location.as_ref();
                StoreKey::try_from(path)
            })
            .collect::<Result<Vec<_>, _>>()?;
        keys.sort();
        prefixes.sort();
        Ok(StoreKeysPrefixes { keys, prefixes })
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let prefix: object_store::path::Path = prefix.as_str().into();
        let mut locations = self.object_store.list(Some(&prefix));
        let mut size = 0;
        while let Some(item) = locations.next().await {
            let meta = item?;
            size += u64::try_from(meta.size).unwrap();
        }
        Ok(size)
    }

    async fn size(&self) -> Result<u64, StorageError> {
        let mut locations = self.object_store.list(None);
        let mut size = 0;
        while let Some(item) = locations.next().await {
            let meta = item?;
            size += u64::try_from(meta.size).unwrap();
        }
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[tokio::test]
    async fn memory() -> Result<(), Box<dyn Error>> {
        let store = AsyncObjectStore::new(object_store::memory::InMemory::new());
        super::super::test_util::store_write(&store).await?;
        super::super::test_util::store_read(&store).await?;
        super::super::test_util::store_list(&store).await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn filesystem() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let store = AsyncObjectStore::new(object_store::local::LocalFileSystem::new_with_prefix(
            path.path(),
        )?);
        super::super::test_util::store_write(&store).await?;
        super::super::test_util::store_read(&store).await?;
        super::super::test_util::store_list(&store).await?;
        Ok(())
    }
}
