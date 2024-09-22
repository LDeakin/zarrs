//! A storage transformer which records performance metrics.

use crate::{
    Bytes, ListableStorageTraits, MaybeBytes, ReadableStorageTraits, StorageError, StoreKey,
    StoreKeyRange, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix,
    WritableStorageTraits,
};

#[cfg(feature = "async")]
use crate::{
    AsyncBytes, AsyncListableStorageTraits, AsyncReadableStorageTraits,
    AsyncReadableWritableStorageTraits, AsyncWritableStorageTraits, MaybeAsyncBytes,
};

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// The performance metrics storage transformer. Accumulates metrics, such as bytes read and written.
///
/// It is intended to aid in testing by allowing the application to validate that metrics (e.g., bytes read/written, total read/write operations) match expected values for specific operations.
#[derive(Debug)]
pub struct PerformanceMetricsStorageAdapter<TStorage: ?Sized> {
    storage: Arc<TStorage>,
    bytes_read: AtomicUsize,
    bytes_written: AtomicUsize,
    reads: AtomicUsize,
    writes: AtomicUsize,
}

impl<TStorage: ?Sized> PerformanceMetricsStorageAdapter<TStorage> {
    /// Create a new performance metrics storage transformer.
    #[must_use]
    pub fn new(storage: Arc<TStorage>) -> Self {
        Self {
            storage,
            bytes_read: AtomicUsize::default(),
            bytes_written: AtomicUsize::default(),
            reads: AtomicUsize::default(),
            writes: AtomicUsize::default(),
        }
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
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for PerformanceMetricsStorageAdapter<TStorage>
{
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let value = self.storage.get(key);
        let bytes_read = value
            .as_ref()
            .map_or(0, |v| v.as_ref().map_or(0, Bytes::len));
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.reads.fetch_add(1, Ordering::Relaxed);
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
            self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
            self.reads.fetch_add(byte_ranges.len(), Ordering::Relaxed);
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
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.reads.fetch_add(key_ranges.len(), Ordering::Relaxed);
        Ok(values)
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        self.storage.size_key(key)
    }
}

impl<TStorage: ?Sized + ListableStorageTraits> ListableStorageTraits
    for PerformanceMetricsStorageAdapter<TStorage>
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
    for PerformanceMetricsStorageAdapter<TStorage>
{
    fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        self.bytes_written.fetch_add(value.len(), Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.storage.set(key, value)
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        let bytes_written = key_start_values
            .iter()
            .map(|ksv| ksv.value().len())
            .sum::<usize>();
        self.bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
        self.writes
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

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncReadableStorageTraits> AsyncReadableStorageTraits
    for PerformanceMetricsStorageAdapter<TStorage>
{
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        let value = self.storage.get(key).await;
        let bytes_read = value
            .as_ref()
            .map_or(0, |v| v.as_ref().map_or(0, AsyncBytes::len));
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.reads.fetch_add(1, Ordering::Relaxed);
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
            self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
            self.reads.fetch_add(byte_ranges.len(), Ordering::Relaxed);
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
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.reads.fetch_add(key_ranges.len(), Ordering::Relaxed);
        Ok(values)
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        self.storage.size_key(key).await
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncListableStorageTraits> AsyncListableStorageTraits
    for PerformanceMetricsStorageAdapter<TStorage>
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
    for PerformanceMetricsStorageAdapter<TStorage>
{
    async fn set(&self, key: &StoreKey, value: AsyncBytes) -> Result<(), StorageError> {
        self.bytes_written.fetch_add(value.len(), Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.storage.set(key, value).await
    }

    async fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        let bytes_written = key_start_values
            .iter()
            .map(|ksv| ksv.value().len())
            .sum::<usize>();
        self.bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
        self.writes
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
    for PerformanceMetricsStorageAdapter<TStorage>
{
}
