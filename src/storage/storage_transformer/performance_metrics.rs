//! A storage transformer which records performance metrics.

use crate::{
    metadata::v3::MetadataV3,
    storage::{
        Bytes, ListableStorage, ListableStorageTraits, MaybeBytes, ReadableListableStorage,
        ReadableStorage, ReadableStorageTraits, ReadableWritableListableStorage,
        ReadableWritableStorage, ReadableWritableStorageTraits, StorageError, StoreKey,
        StoreKeyRange, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix,
        WritableStorage, WritableStorageTraits,
    },
};

#[cfg(feature = "async")]
use crate::storage::{
    AsyncBytes, AsyncListableStorage, AsyncListableStorageTraits, AsyncReadableListableStorage,
    AsyncReadableStorage, AsyncReadableStorageTraits, AsyncReadableWritableListableStorage,
    AsyncReadableWritableStorageTraits, AsyncWritableStorage, AsyncWritableStorageTraits,
    MaybeAsyncBytes,
};

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use super::StorageTransformerExtension;

/// The performance metrics storage transformer. Accumulates metrics, such as bytes read and written.
///
/// This storage transformer is for internal use and will not to be included in `storage_transformers` array metadata.
/// It is intended to aid in testing by allowing the application to validate that metrics (e.g., bytes read/written, total read/write operations, lock requests) match expected values for specific operations.
#[derive(Debug, Default)]
pub struct PerformanceMetricsStorageTransformer {
    bytes_read: AtomicUsize,
    bytes_written: AtomicUsize,
    reads: AtomicUsize,
    writes: AtomicUsize,
    locks: AtomicUsize,
}

impl PerformanceMetricsStorageTransformer {
    /// Create a new performance metrics storage transformer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of bytes read.
    pub fn bytes_read(&self) -> usize {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Returns the number of bytes written.
    pub fn bytes_written(&self) -> usize {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Returns the number of read requests.
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }

    /// Returns the number of write requests.
    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::Relaxed)
    }

    /// Returns the number of lock requests.
    pub fn locks(&self) -> usize {
        self.locks.load(Ordering::Relaxed)
    }

    fn create_transformer<TStorage: ?Sized>(
        self: Arc<Self>,
        storage: Arc<TStorage>,
    ) -> Arc<PerformanceMetricsStorageTransformerImpl<TStorage>> {
        Arc::new(PerformanceMetricsStorageTransformerImpl {
            storage,
            transformer: self,
        })
    }
}

impl StorageTransformerExtension for PerformanceMetricsStorageTransformer {
    /// Returns [`None`], since this storage transformer is not intended to be included in array `storage_transformers` metadata.
    fn create_metadata(&self) -> Option<MetadataV3> {
        None
    }

    fn create_readable_transformer(self: Arc<Self>, storage: ReadableStorage) -> ReadableStorage {
        self.create_transformer(storage)
    }

    fn create_writable_transformer(self: Arc<Self>, storage: WritableStorage) -> WritableStorage {
        self.create_transformer(storage)
    }

    fn create_readable_writable_transformer(
        self: Arc<Self>,
        storage: ReadableWritableStorage,
    ) -> ReadableWritableStorage {
        self.create_transformer(storage)
    }

    fn create_listable_transformer(self: Arc<Self>, storage: ListableStorage) -> ListableStorage {
        self.create_transformer(storage)
    }

    fn create_readable_listable_transformer(
        self: Arc<Self>,
        storage: ReadableListableStorage,
    ) -> ReadableListableStorage {
        self.create_transformer(storage)
    }

    fn create_readable_writable_listable_transformer(
        self: Arc<Self>,
        storage: ReadableWritableListableStorage,
    ) -> ReadableWritableListableStorage {
        self.create_transformer(storage)
    }

    #[cfg(feature = "async")]
    fn create_async_readable_transformer(
        self: Arc<Self>,
        storage: AsyncReadableStorage,
    ) -> AsyncReadableStorage {
        self.create_transformer(storage)
    }

    #[cfg(feature = "async")]
    fn create_async_writable_transformer(
        self: Arc<Self>,
        storage: AsyncWritableStorage,
    ) -> AsyncWritableStorage {
        self.create_transformer(storage)
    }

    #[cfg(feature = "async")]
    fn create_async_listable_transformer(
        self: Arc<Self>,
        storage: AsyncListableStorage,
    ) -> AsyncListableStorage {
        self.create_transformer(storage)
    }

    #[cfg(feature = "async")]
    fn create_async_readable_listable_transformer(
        self: Arc<Self>,
        storage: AsyncReadableListableStorage,
    ) -> AsyncReadableListableStorage {
        self.create_transformer(storage)
    }

    #[cfg(feature = "async")]
    fn create_async_readable_writable_listable_transformer(
        self: Arc<Self>,
        storage: AsyncReadableWritableListableStorage,
    ) -> AsyncReadableWritableListableStorage {
        self.create_transformer(storage)
    }
}

#[derive(Debug)]
struct PerformanceMetricsStorageTransformerImpl<TStorage: ?Sized> {
    storage: Arc<TStorage>,
    transformer: Arc<PerformanceMetricsStorageTransformer>,
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<TStorage>
{
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let value = self.storage.get(key);
        let bytes_read = value
            .as_ref()
            .map_or(0, |v| v.as_ref().map_or(0, Bytes::len));
        self.transformer
            .bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.transformer.reads.fetch_add(1, Ordering::Relaxed);
        value
    }

    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[crate::byte_range::ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        let values = self.storage.get_partial_values_key(key, byte_ranges)?;
        if let Some(values) = &values {
            let bytes_read = values.iter().map(Bytes::len).sum();
            self.transformer
                .bytes_read
                .fetch_add(bytes_read, Ordering::Relaxed);
            self.transformer
                .reads
                .fetch_add(byte_ranges.len(), Ordering::Relaxed);
        }
        Ok(values)
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        let values = self.storage.get_partial_values(key_ranges)?;
        let bytes_read = values
            .iter()
            .map(|value| value.as_ref().map_or(0, Bytes::len))
            .sum::<usize>();
        self.transformer
            .bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.transformer
            .reads
            .fetch_add(key_ranges.len(), Ordering::Relaxed);
        Ok(values)
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        self.storage.size_key(key)
    }
}

impl<TStorage: ?Sized + ListableStorageTraits> ListableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<TStorage>
{
    fn list(&self) -> Result<StoreKeys, StorageError> {
        self.storage.list()
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        self.storage.list_prefix(prefix)
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        self.storage.list_dir(prefix)
    }

    fn size(&self) -> Result<u64, StorageError> {
        self.storage.size()
    }

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        self.storage.size_prefix(prefix)
    }
}

impl<TStorage: ?Sized + WritableStorageTraits> WritableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<TStorage>
{
    fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        self.transformer
            .bytes_written
            .fetch_add(value.len(), Ordering::Relaxed);
        self.transformer.writes.fetch_add(1, Ordering::Relaxed);
        self.storage.set(key, value)
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        let bytes_written = key_start_values
            .iter()
            .map(|ksv| ksv.value.len())
            .sum::<usize>();
        self.transformer
            .bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
        self.transformer
            .writes
            .fetch_add(key_start_values.len(), Ordering::Relaxed);
        self.storage.set_partial_values(key_start_values)
    }

    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        self.storage.erase(key)
    }

    fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        self.storage.erase_values(keys)
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        self.storage.erase_prefix(prefix)
    }
}

impl<TStorage: ?Sized + ReadableWritableStorageTraits> ReadableWritableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<TStorage>
{
    // fn mutex(&self, key: &StoreKey) -> Result<StoreKeyMutex, StorageError> {
    //     self.transformer.locks.fetch_add(1, Ordering::Relaxed);
    //     self.storage.mutex(key)
    // }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncReadableStorageTraits> AsyncReadableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<TStorage>
{
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        let value = self.storage.get(key).await;
        let bytes_read = value
            .as_ref()
            .map_or(0, |v| v.as_ref().map_or(0, AsyncBytes::len));
        self.transformer
            .bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.transformer.reads.fetch_add(1, Ordering::Relaxed);
        value
    }

    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[crate::byte_range::ByteRange],
    ) -> Result<Option<Vec<AsyncBytes>>, StorageError> {
        let values = self
            .storage
            .get_partial_values_key(key, byte_ranges)
            .await?;
        if let Some(values) = &values {
            let bytes_read = values.iter().map(AsyncBytes::len).sum();
            self.transformer
                .bytes_read
                .fetch_add(bytes_read, Ordering::Relaxed);
            self.transformer
                .reads
                .fetch_add(byte_ranges.len(), Ordering::Relaxed);
        }
        Ok(values)
    }

    async fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeAsyncBytes>, StorageError> {
        let values = self.storage.get_partial_values(key_ranges).await?;
        let bytes_read = values
            .iter()
            .map(|value| value.as_ref().map_or(0, AsyncBytes::len))
            .sum::<usize>();
        self.transformer
            .bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.transformer
            .reads
            .fetch_add(key_ranges.len(), Ordering::Relaxed);
        Ok(values)
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        self.storage.size_key(key).await
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncListableStorageTraits> AsyncListableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<TStorage>
{
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        self.storage.list().await
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        self.storage.list_prefix(prefix).await
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        self.storage.list_dir(prefix).await
    }

    async fn size(&self) -> Result<u64, StorageError> {
        self.storage.size().await
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        self.storage.size_prefix(prefix).await
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncWritableStorageTraits> AsyncWritableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<TStorage>
{
    async fn set(&self, key: &StoreKey, value: AsyncBytes) -> Result<(), StorageError> {
        self.transformer
            .bytes_written
            .fetch_add(value.len(), Ordering::Relaxed);
        self.transformer.writes.fetch_add(1, Ordering::Relaxed);
        self.storage.set(key, value).await
    }

    async fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        let bytes_written = key_start_values
            .iter()
            .map(|ksv| ksv.value.len())
            .sum::<usize>();
        self.transformer
            .bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
        self.transformer
            .writes
            .fetch_add(key_start_values.len(), Ordering::Relaxed);
        self.storage.set_partial_values(key_start_values).await
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        self.storage.erase(key).await
    }

    async fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        self.storage.erase_values(keys).await
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        self.storage.erase_prefix(prefix).await
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncReadableWritableStorageTraits> AsyncReadableWritableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<TStorage>
{
    // async fn mutex(&self, key: &StoreKey) -> Result<AsyncStoreKeyMutex, StorageError> {
    //     self.transformer.locks.fetch_add(1, Ordering::Relaxed);
    //     self.storage.mutex(key).await
    // }
}
