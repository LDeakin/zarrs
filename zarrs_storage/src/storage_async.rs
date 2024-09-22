use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;

use super::{
    byte_range::ByteRange, AsyncBytes, MaybeAsyncBytes, StorageError, StoreKey, StoreKeyRange,
    StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix, StorePrefixes,
};

/// Async readable storage traits.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait AsyncReadableStorageTraits: Send + Sync {
    /// Retrieve the value (bytes) associated with a given [`StoreKey`].
    ///
    /// Returns [`None`] if the key is not found.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the store key does not exist or there is an error with the underlying store.
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        Ok(self
            .get_partial_values_key(key, &[ByteRange::FromStart(0, None)])
            .await?
            .map(|mut v| v.remove(0)))
    }

    /// Retrieve partial bytes from a list of byte ranges for a store key.
    ///
    /// Returns [`None`] if the key is not found.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<AsyncBytes>>, StorageError>;

    /// Retrieve partial bytes from a list of [`StoreKeyRange`].
    ///
    /// # Parameters
    /// * `key_ranges`: ordered set of ([`StoreKey`], [`ByteRange`]) pairs. A key may occur multiple times with different ranges.
    ///
    /// # Output
    /// A a list of values in the order of the `key_ranges`. It will be [`None`] for missing keys.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeAsyncBytes>, StorageError> {
        self.get_partial_values_batched_by_key(key_ranges).await
    }

    /// Return the size in bytes of the value at `key`.
    ///
    /// Returns [`None`] if the key is not found.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError>;

    /// A utility method with the same input and output as [`get_partial_values`](AsyncReadableStorageTraits::get_partial_values) that internally calls [`get_partial_values_key`](AsyncReadableStorageTraits::get_partial_values_key) with byte ranges grouped by key.
    ///
    /// Readable storage can use this function in the implementation of [`get_partial_values`](AsyncReadableStorageTraits::get_partial_values) if that is optimal.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn get_partial_values_batched_by_key(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeAsyncBytes>, StorageError> {
        let mut out: Vec<MaybeAsyncBytes> = Vec::with_capacity(key_ranges.len());
        let mut last_key = None;
        let mut byte_ranges_key = Vec::new();
        for key_range in key_ranges {
            if last_key.is_none() {
                last_key = Some(&key_range.key);
            }
            let last_key_val = last_key.unwrap();

            if key_range.key != *last_key_val {
                // Found a new key, so do a batched get of the byte ranges of the last key
                let bytes = (self
                    .get_partial_values_key(last_key.unwrap(), &byte_ranges_key)
                    .await?)
                    .map_or_else(
                        || vec![None; byte_ranges_key.len()],
                        |partial_values| partial_values.into_iter().map(Some).collect(),
                    );
                out.extend(bytes);
                last_key = Some(&key_range.key);
                byte_ranges_key.clear();
            }

            byte_ranges_key.push(key_range.byte_range);
        }

        if !byte_ranges_key.is_empty() {
            // Get the byte ranges of the last key
            let bytes = (self
                .get_partial_values_key(last_key.unwrap(), &byte_ranges_key)
                .await?)
                .map_or_else(
                    || vec![None; byte_ranges_key.len()],
                    |partial_values| partial_values.into_iter().map(Some).collect(),
                );
            out.extend(bytes);
        }

        Ok(out)
    }
}

/// Async listable storage traits.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait AsyncListableStorageTraits: Send + Sync {
    /// Retrieve all [`StoreKeys`] in the store.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying error with the store.
    async fn list(&self) -> Result<StoreKeys, StorageError>;

    /// Retrieve all [`StoreKeys`] with a given [`StorePrefix`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the prefix is not a directory or there is an underlying error with the store.
    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError>;

    /// Retrieve all [`StoreKeys`] and [`StorePrefix`] which are direct children of [`StorePrefix`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the prefix is not a directory or there is an underlying error with the store.
    ///
    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError>;

    /// Return the size in bytes of all keys under `prefix`.
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if the store does not support size() or there is an underlying error with the store.
    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError>;

    /// Return the size in bytes of the storage.
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if the store does not support size() or there is an underlying error with the store.
    async fn size(&self) -> Result<u64, StorageError> {
        self.size_prefix(&StorePrefix::root()).await
    }
}

/// Set partial values for an asynchronous store.
///
/// This method reads entire values, updates them, and replaces them.
/// Stores can use this internally if they do not support updating/appending without replacement.
///
/// # Errors
/// Returns a [`StorageError`] if an underlying store operation fails.
///
/// # Panics
/// Panics if a key ends beyond `usize::MAX`.
pub async fn async_store_set_partial_values<T: AsyncReadableWritableStorageTraits>(
    store: &T,
    key_start_values: &[StoreKeyStartValue<'_>],
    // truncate: bool
) -> Result<(), StorageError> {
    let groups = key_start_values
        .iter()
        .chunk_by(|key_start_value| &key_start_value.key)
        .into_iter()
        .map(|(key, group)| (key, group.into_iter().cloned().collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    futures::stream::iter(&groups)
        .map(Ok)
        .try_for_each_concurrent(None, |(key, group)| async move {
            // Lock the store key
            // let mutex = store.mutex(&key).await?;
            // let _lock = mutex.lock().await;

            // Read the store key
            let bytes = store.get(key).await?.unwrap_or_default();
            let mut vec = Vec::<u8>::from(bytes);

            // Expand the store key if needed
            let end_max =
                usize::try_from(group.iter().map(StoreKeyStartValue::end).max().unwrap()).unwrap();
            if vec.len() < end_max {
                vec.resize_with(end_max, Default::default);
            }
            // else if truncate {
            //     vec.truncate(end_max);
            // };

            // Update the store key
            for key_start_value in group {
                let start: usize = key_start_value.start.try_into().unwrap();
                let end: usize = key_start_value.end().try_into().unwrap();
                vec[start..end].copy_from_slice(key_start_value.value);
            }

            // Write the store key
            store.set(key, vec.into()).await
        })
        .await
}

/// Async writable storage traits.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait AsyncWritableStorageTraits: Send + Sync {
    /// Store bytes at a [`StoreKey`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] on failure to store.
    async fn set(&self, key: &StoreKey, value: AsyncBytes) -> Result<(), StorageError>;

    /// Store bytes according to a list of [`StoreKeyStartValue`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] on failure to store.
    async fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError>;

    /// Erase a [`StoreKey`].
    ///
    /// Succeeds if the key does not exist.
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError>;

    /// Erase a list of [`StoreKey`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        let futures_erase = keys.iter().map(|key| self.erase(key));
        futures::future::join_all(futures_erase)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    /// Erase all [`StoreKey`] under [`StorePrefix`].
    ///
    /// # Errors
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError>;
}

/// A supertrait of [`AsyncReadableStorageTraits`] and [`AsyncWritableStorageTraits`].
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait AsyncReadableWritableStorageTraits:
    AsyncReadableStorageTraits + AsyncWritableStorageTraits
{
}

// FIXME: Enable this in the next major release
// impl<T> AsyncReadableWritableStorageTraits for T where
//     T: AsyncReadableStorageTraits + AsyncWritableStorageTraits
// {
// }

/// A supertrait of [`AsyncReadableStorageTraits`] and [`AsyncListableStorageTraits`].
pub trait AsyncReadableListableStorageTraits:
    AsyncReadableStorageTraits + AsyncListableStorageTraits
{
}

impl<T> AsyncReadableListableStorageTraits for T where
    T: AsyncReadableStorageTraits + AsyncListableStorageTraits
{
}

/// A supertrait of [`AsyncReadableWritableStorageTraits`] and [`AsyncListableStorageTraits`].
pub trait AsyncReadableWritableListableStorageTraits:
    AsyncReadableWritableStorageTraits + AsyncListableStorageTraits
{
}

impl<T> AsyncReadableWritableListableStorageTraits for T where
    T: AsyncReadableWritableStorageTraits + AsyncListableStorageTraits
{
}

/// Asynchronously discover the children of a store prefix.
///
/// # Errors
/// Returns a [`StorageError`] if there is an underlying error with the store.
pub async fn async_discover_children<
    TStorage: ?Sized + AsyncReadableStorageTraits + AsyncListableStorageTraits,
>(
    storage: &Arc<TStorage>,
    prefix: &StorePrefix,
) -> Result<StorePrefixes, StorageError> {
    let children: Result<Vec<_>, _> = storage
        .list_dir(prefix)
        .await?
        .prefixes()
        .iter()
        .filter(|v| !v.as_str().starts_with("__"))
        .map(|v| StorePrefix::new(v.as_str()))
        .collect();
    Ok(children?)
}
