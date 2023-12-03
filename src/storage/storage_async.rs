use async_trait::async_trait;
use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{array::MaybeBytes, byte_range::ByteRange};

use super::{
    StorageError, StoreKey, StoreKeyRange, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes,
    StorePrefix,
};

/// Async readable storage traits.
#[async_trait]
pub trait AsyncReadableStorageTraits: Send + Sync {
    /// Retrieve the value (bytes) associated with a given [`StoreKey`].
    ///
    /// Returns [`None`] if the key is not found.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if the store key does not exist or there is an error with the underlying store.
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError>;

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
    ) -> Result<Option<Vec<Vec<u8>>>, StorageError>;

    /// Retrieve partial bytes from a list of [`StoreKeyRange`].
    ///
    /// # Arguments
    /// * `key_ranges`: ordered set of ([`StoreKey`], [`ByteRange`]) pairs. A key may occur multiple times with different ranges.
    ///
    /// # Output
    ///
    /// A a list of values in the order of the `key_ranges`. It will be [`None`] for missing keys.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError>;

    /// Return the size in bytes of all keys under `prefix`.
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if the store does not support size() or there is an underlying error with the store.
    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError>;

    /// Return the size in bytes of the value at `key`.
    ///
    /// Returns [`None`] if the key is not found.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError>;

    /// Return the size in bytes of the readable storage.
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if the store does not support size() or there is an underlying error with the store.
    async fn size(&self) -> Result<u64, StorageError>;

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
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        let mut out: Vec<MaybeBytes> = Vec::with_capacity(key_ranges.len());
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
#[async_trait]
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
}

/// Async writable storage traits.
#[async_trait]
pub trait AsyncWritableStorageTraits: Send + Sync + AsyncReadableStorageTraits {
    /// Store bytes at a [`StoreKey`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] on failure to store.
    async fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError>;

    /// Store bytes according to a list of [`StoreKeyStartValue`].
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] on failure to store.
    async fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        // Group by key
        let group_by_key = key_start_values
            .iter()
            .group_by(|key_start_value| &key_start_value.key)
            .into_iter()
            .map(|(key, group)| (key.clone(), group.into_iter().cloned().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        // Read keys
        let mut futures = group_by_key
            .into_iter()
            .map(|(key, group)| async move {
                let mut bytes = self.get(&key.clone()).await?.unwrap_or_else(Vec::default);
                let end_max =
                    usize::try_from(group.iter().map(StoreKeyStartValue::end).max().unwrap())
                        .unwrap();

                // Expand the store key if needed
                if bytes.len() < end_max {
                    bytes.resize_with(end_max, Default::default);
                }

                // Update the store key
                for key_start_value in group {
                    let start: usize = key_start_value.start.try_into().unwrap();
                    let end: usize = key_start_value.end().try_into().unwrap();
                    bytes[start..end].copy_from_slice(key_start_value.value);
                }

                // Write the store key
                self.set(&key, &bytes).await
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(item) = futures.next().await {
            item?;
        }

        Ok(())
    }

    /// Erase a [`StoreKey`].
    ///
    /// Returns true if the key exists and was erased, or false if the key does not exist.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn erase(&self, key: &StoreKey) -> Result<bool, StorageError>;

    /// Erase a list of [`StoreKey`].
    ///
    /// Returns true if all keys existed and were erased, or false if any key does not exist.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if there is an underlying storage error.
    async fn erase_values(&self, keys: &[StoreKey]) -> Result<bool, StorageError> {
        let futures_erase = keys.iter().map(|key| self.erase(key));
        let result = futures::future::join_all(futures_erase)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let all_deleted = result.iter().all(|b| *b);
        Ok(all_deleted)
    }

    /// Erase all [`StoreKey`] under [`StorePrefix`].
    ///
    /// Returns true if the prefix and all its children were removed.
    ///
    /// # Errors
    /// Returns a [`StorageError`] is the prefix is not in the store, or the erase otherwise fails.
    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<bool, StorageError>;
}

/// A supertrait of [`AsyncReadableStorageTraits`] and [`AsyncListableStorageTraits`].
pub trait AsyncReadableListableStorageTraits:
    AsyncReadableStorageTraits + AsyncListableStorageTraits
{
}

impl<T> AsyncReadableListableStorageTraits for T where
    T: AsyncReadableStorageTraits + AsyncListableStorageTraits
{
}
