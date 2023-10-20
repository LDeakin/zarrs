//! A storage transformer which records performance metrics.

use crate::{
    array::MaybeBytes,
    metadata::Metadata,
    storage::{
        ListableStorage, ListableStorageTraits, ReadableListableStorage,
        ReadableListableStorageTraits, ReadableStorage, ReadableStorageTraits,
        ReadableWritableStorage, ReadableWritableStorageTraits, StorageError, StoreKey,
        StoreKeyRange, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes, StorePrefix,
        WritableStorage, WritableStorageTraits,
    },
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use super::StorageTransformerExtension;

/// The performance metrics storage transformer.
#[derive(Debug, Default)]
pub struct PerformanceMetricsStorageTransformer {
    bytes_read: AtomicUsize,
    bytes_written: AtomicUsize,
    reads: AtomicUsize,
    writes: AtomicUsize,
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

    fn create_transformer<TStorage: ?Sized>(
        &self,
        storage: Arc<TStorage>,
    ) -> Arc<PerformanceMetricsStorageTransformerImpl<TStorage>> {
        Arc::new(PerformanceMetricsStorageTransformerImpl {
            storage,
            transformer: self,
        })
    }
}

impl StorageTransformerExtension for PerformanceMetricsStorageTransformer {
    fn create_metadata(&self) -> Option<Metadata> {
        None
    }

    fn create_readable_transformer<'a>(
        &'a self,
        storage: ReadableStorage<'a>,
    ) -> ReadableStorage<'a> {
        self.create_transformer(storage)
    }

    fn create_writable_transformer<'a>(
        &'a self,
        storage: WritableStorage<'a>,
    ) -> WritableStorage<'a> {
        self.create_transformer(storage)
    }

    fn create_listable_transformer<'a>(
        &'a self,
        storage: ListableStorage<'a>,
    ) -> ListableStorage<'a> {
        self.create_transformer(storage)
    }

    fn create_readable_writable_transformer<'a>(
        &'a self,
        storage: ReadableWritableStorage<'a>,
    ) -> ReadableWritableStorage<'a> {
        self.create_transformer(storage)
    }

    fn create_readable_listable_transformer<'a>(
        &'a self,
        storage: ReadableListableStorage<'a>,
    ) -> ReadableListableStorage<'a> {
        self.create_transformer(storage)
    }
}

#[derive(Debug)]
struct PerformanceMetricsStorageTransformerImpl<'a, TStorage: ?Sized> {
    storage: Arc<TStorage>,
    transformer: &'a PerformanceMetricsStorageTransformer,
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<'_, TStorage>
{
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let value = self.storage.get(key);
        let bytes_read = value
            .as_ref()
            .map_or(0, |v| v.as_ref().map_or(0, std::vec::Vec::len));
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
    ) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        let values = self.storage.get_partial_values_key(key, byte_ranges)?;
        if let Some(values) = &values {
            let bytes_read = values.iter().map(Vec::len).sum();
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
            .map(|value| value.as_ref().map_or(0, Vec::len))
            .sum::<usize>();
        self.transformer
            .bytes_read
            .fetch_add(bytes_read, Ordering::Relaxed);
        self.transformer
            .reads
            .fetch_add(key_ranges.len(), Ordering::Relaxed);
        Ok(values)
    }

    fn size(&self) -> Result<u64, StorageError> {
        self.storage.size()
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        self.storage.size_key(key)
    }
}

impl<TStorage: ?Sized + ListableStorageTraits> ListableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<'_, TStorage>
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
}

impl<TStorage: ?Sized + WritableStorageTraits> WritableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<'_, TStorage>
{
    fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError> {
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

    fn erase(&self, key: &StoreKey) -> Result<bool, StorageError> {
        self.storage.erase(key)
    }

    fn erase_values(&self, keys: &[StoreKey]) -> Result<bool, StorageError> {
        self.storage.erase_values(keys)
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<bool, StorageError> {
        self.storage.erase_prefix(prefix)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits + WritableStorageTraits> ReadableWritableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<'_, TStorage>
{
}

impl<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits> ReadableListableStorageTraits
    for PerformanceMetricsStorageTransformerImpl<'_, TStorage>
{
}
