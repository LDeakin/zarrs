use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore};

use crate::{
    array::MaybeBytes,
    byte_range::ByteRange,
    storage::{
        AsyncListableStorageTraits, AsyncReadableStorageTraits, AsyncWritableStorageTraits,
        StorageError, StoreKey, StoreKeyRange, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes,
        StorePrefix,
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

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl<T: ObjectStore> AsyncReadableStorageTraits for T {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let get = handle_result(ObjectStore::get(self, &key_to_path(key)).await)?;
        if let Some(get) = get {
            let bytes = get.bytes().await?;
            Ok(Some(bytes.to_vec()))
        } else {
            Ok(None)
        }
    }

    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        let Some(size) = self.size_key(key).await? else {
            return Ok(None);
        };
        let ranges = byte_ranges
            .iter()
            .map(|byte_range| byte_range.to_range_usize(size))
            .collect::<Vec<_>>();
        let get_ranges = self.get_ranges(&key_to_path(key), &ranges).await;
        match get_ranges {
            Ok(get_ranges) => Ok(Some(
                get_ranges.iter().map(|bytes| bytes.to_vec()).collect(),
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

    async fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        self.get_partial_values_batched_by_key(key_ranges).await
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let prefix: object_store::path::Path = prefix.as_str().into();
        let mut locations = ObjectStore::list(self, Some(&prefix));
        let mut size = 0;
        while let Some(item) = locations.next().await {
            let meta = item?;
            size += u64::try_from(meta.size).unwrap();
        }
        Ok(size)
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        Ok(handle_result(self.head(&key_to_path(key)).await)?.map(|meta| meta.size as u64))
    }

    async fn size(&self) -> Result<u64, StorageError> {
        let mut locations = ObjectStore::list(self, None);
        let mut size = 0;
        while let Some(item) = locations.next().await {
            let meta = item?;
            size += u64::try_from(meta.size).unwrap();
        }
        Ok(size)
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl<T: ObjectStore> AsyncWritableStorageTraits for T {
    async fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError> {
        // FIXME: Can this copy be avoided?
        let bytes = bytes::Bytes::copy_from_slice(value);
        ObjectStore::put(self, &key_to_path(key), bytes).await?;
        Ok(())
    }

    async fn set_partial_values(
        &self,
        _key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        // This is implemented in the parent
        unreachable!()
    }

    async fn erase(&self, key: &StoreKey) -> Result<bool, StorageError> {
        Ok(handle_result(ObjectStore::delete(self, &key_to_path(key)).await)?.is_some())
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<bool, StorageError> {
        let prefix: object_store::path::Path = prefix.as_str().into();
        let locations = ObjectStore::list(self, Some(&prefix))
            .map_ok(|m| m.location)
            .boxed();
        ObjectStore::delete_stream(self, locations)
            .try_collect::<Vec<Path>>()
            .await?;
        Ok(true)
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl<T: ObjectStore> AsyncListableStorageTraits for T {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        let mut list = ObjectStore::list(self, None)
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
        let mut list = ObjectStore::list(self, Some(&path))
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
        let list_result = ObjectStore::list_with_delimiter(self, Some(&path)).await?;
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
}

/// Implement the storage traits for an object store
#[macro_export]
macro_rules! object_store_impl {
    ($store:ty, $object_store:ident, $locks:ident) => {
        #[cfg_attr(feature = "async", async_trait::async_trait)]
        impl $crate::storage::AsyncReadableStorageTraits for $store {
            async fn get(
                &self,
                key: &$crate::storage::StoreKey,
            ) -> Result<$crate::array::MaybeBytes, $crate::storage::StorageError> {
                $crate::storage::AsyncReadableStorageTraits::get(&self.$object_store, key).await
            }

            async fn get_partial_values_key(
                &self,
                key: &$crate::storage::StoreKey,
                byte_ranges: &[$crate::storage::ByteRange],
            ) -> Result<Option<Vec<Vec<u8>>>, $crate::storage::StorageError> {
                $crate::storage::AsyncReadableStorageTraits::get_partial_values_key(
                    &self.$object_store,
                    key,
                    byte_ranges,
                )
                .await
            }

            async fn get_partial_values(
                &self,
                key_ranges: &[$crate::storage::StoreKeyRange],
            ) -> Result<Vec<$crate::array::MaybeBytes>, $crate::storage::StorageError> {
                $crate::storage::AsyncReadableStorageTraits::get_partial_values(
                    &self.object_store,
                    key_ranges,
                )
                .await
            }

            async fn size_prefix(
                &self,
                prefix: &$crate::storage::StorePrefix,
            ) -> Result<u64, $crate::storage::StorageError> {
                $crate::storage::AsyncReadableStorageTraits::size_prefix(
                    &self.$object_store,
                    prefix,
                )
                .await
            }

            async fn size_key(
                &self,
                key: &$crate::storage::StoreKey,
            ) -> Result<Option<u64>, $crate::storage::StorageError> {
                $crate::storage::AsyncReadableStorageTraits::size_key(&self.$object_store, key)
                    .await
            }

            async fn size(&self) -> Result<u64, $crate::storage::StorageError> {
                $crate::storage::AsyncReadableStorageTraits::size(&self.$object_store).await
            }
        }

        #[cfg_attr(feature = "async", async_trait::async_trait)]
        impl $crate::storage::AsyncWritableStorageTraits for $store {
            async fn set(
                &self,
                key: &$crate::storage::StoreKey,
                value: &[u8],
            ) -> Result<(), $crate::storage::StorageError> {
                $crate::storage::AsyncWritableStorageTraits::set(&self.$object_store, key, value)
                    .await
            }

            async fn set_partial_values(
                &self,
                key_start_values: &[$crate::storage::StoreKeyStartValue],
            ) -> Result<(), $crate::storage::StorageError> {
                $crate::storage::async_store_set_partial_values(self, key_start_values).await
            }

            async fn erase(
                &self,
                key: &$crate::storage::StoreKey,
            ) -> Result<bool, $crate::storage::StorageError> {
                $crate::storage::AsyncWritableStorageTraits::erase(&self.$object_store, key).await
            }

            async fn erase_prefix(
                &self,
                prefix: &$crate::storage::StorePrefix,
            ) -> Result<bool, $crate::storage::StorageError> {
                $crate::storage::AsyncWritableStorageTraits::erase_prefix(
                    &self.$object_store,
                    prefix,
                )
                .await
            }
        }

        #[cfg_attr(feature = "async", async_trait::async_trait)]
        impl $crate::storage::AsyncReadableWritableStorageTraits for $store {
            async fn mutex(
                &self,
                key: &$crate::storage::StoreKey,
            ) -> Result<
                $crate::storage::store_lock::AsyncStoreKeyMutex,
                $crate::storage::StorageError,
            > {
                let mutex = self.$locks.mutex(key).await;
                Ok(mutex)
            }
        }

        #[cfg_attr(feature = "async", async_trait::async_trait)]
        impl $crate::storage::AsyncListableStorageTraits for $store {
            async fn list(
                &self,
            ) -> Result<$crate::storage::StoreKeys, $crate::storage::StorageError> {
                $crate::storage::AsyncListableStorageTraits::list(&self.$object_store).await
            }

            async fn list_prefix(
                &self,
                prefix: &$crate::storage::StorePrefix,
            ) -> Result<$crate::storage::StoreKeys, $crate::storage::StorageError> {
                $crate::storage::AsyncListableStorageTraits::list_prefix(
                    &self.$object_store,
                    prefix,
                )
                .await
            }

            async fn list_dir(
                &self,
                prefix: &$crate::storage::StorePrefix,
            ) -> Result<$crate::storage::StoreKeysPrefixes, $crate::storage::StorageError> {
                $crate::storage::AsyncListableStorageTraits::list_dir(&self.$object_store, prefix)
                    .await
            }
        }
    };
}
