//! A storage transformer which prints function calls.

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use itertools::Itertools;

use crate::{
    byte_range::ByteRange,
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

use super::StorageTransformerExtension;

/// The usage log storage transformer. Logs storage method calls.
///
/// This storage transformer is for internal use and will not to be included in `storage_transformers` array metadata.
/// It is intended to aid in debugging and optimising performance by revealing storage access patterns.
///
/// ### Example (log to stdout)
/// ```rust
/// # use std::sync::{Arc, Mutex};
/// # use zarrs::storage::store::MemoryStore;
/// # use zarrs::storage::storage_transformer::{UsageLogStorageTransformer, StorageTransformerExtension};
/// let store = Arc::new(MemoryStore::new());
/// let log_writer = Arc::new(Mutex::new(
///     // std::io::BufWriter::new(
///     std::io::stdout(),
///     //    )
/// ));
/// let usage_log = Arc::new(UsageLogStorageTransformer::new(log_writer, || {
///     chrono::Utc::now().format("[%T%.3f] ").to_string()
/// }));
/// let store = usage_log.create_readable_writable_transformer(store);
/// ````
///
/// Applying array methods with the above [`UsageLogStorageTransformer`] prints outputs like:
/// ```text
/// [23:41:19.885] set(group/array/c/1/0, len=140) -> Ok(())
/// [23:41:19.885] get_partial_values_key(group/array/c/0/0, [-36..-0]) -> len=Ok([36])
/// [23:41:19.886] get_partial_values_key(group/array/c/0/0, [52..104]) -> len=Ok([52])
/// [23:41:19.887] get(group/array/c/1/0) -> len=Ok(140)
/// [23:41:19.891] get(zarr.json) -> len=Ok(0)
/// [23:41:19.891] list_dir() -> (keys:[], prefixes:[group/])
/// [23:41:19.891] get(group/zarr.json) -> len=Ok(86)
/// [23:41:19.891] list_dir(group/) -> (keys:[group/zarr.json], prefixes:[group/array/])
/// [23:41:19.891] get(group/array/zarr.json) -> len=Ok(1315)
/// [23:41:19.892] list() -> [group/array/c/0/0, group/array/c/1/0, group/array/zarr.json, group/zarr.json]
/// ```
pub struct UsageLogStorageTransformer {
    handle: Arc<Mutex<dyn Write + Send + Sync>>,
    prefix_func: fn() -> String,
}

impl core::fmt::Debug for UsageLogStorageTransformer {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        writeln!(f, "usage log")
    }
}

impl UsageLogStorageTransformer {
    /// Create a new usage log storage transformer.
    pub fn new(handle: Arc<Mutex<dyn Write + Send + Sync>>, prefix_func: fn() -> String) -> Self {
        Self {
            handle,
            prefix_func,
        }
    }

    fn create_transformer<TStorage: ?Sized>(
        &self,
        storage: Arc<TStorage>,
    ) -> Arc<UsageLogStorageTransformerImpl<TStorage>> {
        Arc::new(UsageLogStorageTransformerImpl {
            storage,
            prefix_func: self.prefix_func,
            handle: self.handle.clone(),
        })
    }
}

impl StorageTransformerExtension for UsageLogStorageTransformer {
    /// Returns [`None`], since this storage transformer is not intended to be included in array `storage_transformers` metadata.
    fn create_metadata(&self) -> Option<MetadataV3> {
        None
    }

    fn create_readable_transformer(self: Arc<Self>, storage: ReadableStorage) -> ReadableStorage {
        self.create_transformer(storage)
    }

    fn create_readable_writable_transformer(
        self: Arc<Self>,
        storage: ReadableWritableStorage,
    ) -> ReadableWritableStorage {
        self.create_transformer(storage)
    }

    fn create_writable_transformer(self: Arc<Self>, storage: WritableStorage) -> WritableStorage {
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

struct UsageLogStorageTransformerImpl<TStorage: ?Sized> {
    storage: Arc<TStorage>,
    prefix_func: fn() -> String,
    handle: Arc<Mutex<dyn Write + Send + Sync>>,
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let result = self.storage.get(key);
        writeln!(
            self.handle.lock().unwrap(),
            "{}get({key}) -> len={:?}",
            (self.prefix_func)(),
            result.as_ref().map(|v| v.as_ref().map_or(0, Bytes::len))
        )?;
        result
    }

    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        let result = self.storage.get_partial_values_key(key, byte_ranges);
        writeln!(
            self.handle.lock().unwrap(),
            "{}get_partial_values_key({key}, [{}]) -> len={:?}",
            (self.prefix_func)(),
            byte_ranges.iter().format(", "),
            result.as_ref().map(|v| {
                v.as_ref()
                    .map_or(vec![], |v| v.iter().map(Bytes::len).collect_vec())
            })
        )?;
        result
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        let result = self.storage.get_partial_values(key_ranges);
        writeln!(
            self.handle.lock().unwrap(),
            "{}get_partial_values([{}]) -> len={:?}",
            (self.prefix_func)(),
            key_ranges.iter().format(", "),
            result
                .as_ref()
                .map(|v| { v.iter().map(|v| v.iter().map(Bytes::len).collect_vec()) })
        )?;
        result
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let result = self.storage.size_key(key);
        writeln!(
            self.handle.lock().unwrap(),
            "{}size_key({key}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }
}

impl<TStorage: ?Sized + ListableStorageTraits> ListableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    fn list(&self) -> Result<StoreKeys, StorageError> {
        let result = self.storage.list();
        writeln!(
            self.handle.lock().unwrap(),
            "{}list() -> [{}]",
            (self.prefix_func)(),
            result.as_ref().unwrap_or(&vec![]).iter().format(", ")
        )?;
        result
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let result = self.storage.list_prefix(prefix);
        writeln!(
            self.handle.lock().unwrap(),
            "{}list_prefix({prefix}) -> [{}]",
            (self.prefix_func)(),
            result.as_ref().unwrap_or(&vec![]).iter().format(", ")
        )?;
        result
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let result = self.storage.list_dir(prefix);
        writeln!(
            self.handle.lock().unwrap(),
            "{}list_dir({prefix}) -> (keys:[{}], prefixes:[{}])",
            (self.prefix_func)(),
            result.as_ref().map_or(String::new(), |skp| skp
                .keys()
                .iter()
                .format(", ")
                .to_string()),
            result.as_ref().map_or(String::new(), |skp| skp
                .prefixes()
                .iter()
                .format(", ")
                .to_string()),
        )?;
        result
    }

    fn size(&self) -> Result<u64, StorageError> {
        let result = self.storage.size();
        writeln!(
            self.handle.lock().unwrap(),
            "{}size() -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let result: Result<u64, StorageError> = self.storage.size_prefix(prefix);
        writeln!(
            self.handle.lock().unwrap(),
            "{}size_prefix({prefix}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }
}

impl<TStorage: ?Sized + WritableStorageTraits> WritableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        let len = value.len();
        let result = self.storage.set(key, value);
        writeln!(
            self.handle.lock().unwrap(),
            "{}set({key}, len={}) -> {result:?}",
            (self.prefix_func)(),
            len
        )?;
        result
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        let result = self.storage.set_partial_values(key_start_values);
        writeln!(
            self.handle.lock().unwrap(),
            "{}set_partial_values({key_start_values:?}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }

    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        let result = self.storage.erase(key);
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase({key}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }

    fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        let result = self.storage.erase_values(keys);
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase_values([{}]) -> {result:?}",
            keys.iter().format(", "),
            (self.prefix_func)()
        )?;
        result
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        let result = self.storage.erase_prefix(prefix);
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase_prefix({prefix}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }
}

impl<TStorage: ?Sized + ReadableWritableStorageTraits> ReadableWritableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    // fn mutex(&self, key: &StoreKey) -> Result<StoreKeyMutex, StorageError> {
    //     let result = self.storage.mutex(key);
    //     writeln!(
    //         self.handle.lock().unwrap(),
    //         "{}mutex({key}) -> {:?}",
    //         (self.prefix_func)(),
    //         result.as_ref().map_or((), |_| ())
    //     )?;
    //     result
    // }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncReadableStorageTraits> AsyncReadableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    async fn get(&self, key: &StoreKey) -> Result<MaybeAsyncBytes, StorageError> {
        let result = self.storage.get(key).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}get({key}) -> len={:?}",
            (self.prefix_func)(),
            result
                .as_ref()
                .map(|v| v.as_ref().map_or(0, AsyncBytes::len))
        )?;
        result
    }

    async fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<AsyncBytes>>, StorageError> {
        let result = self.storage.get_partial_values_key(key, byte_ranges).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}get_partial_values_key({key}, [{}]) -> len={:?}",
            (self.prefix_func)(),
            byte_ranges.iter().format(", "),
            result.as_ref().map(|v| {
                v.as_ref()
                    .map_or(vec![], |v| v.iter().map(AsyncBytes::len).collect_vec())
            })
        )?;
        result
    }

    async fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeAsyncBytes>, StorageError> {
        let result = self.storage.get_partial_values(key_ranges).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}get_partial_values([{}]) -> len={:?}",
            (self.prefix_func)(),
            key_ranges.iter().format(", "),
            result.as_ref().map(|v| {
                v.iter()
                    .map(|v| v.iter().map(AsyncBytes::len).collect_vec())
            })
        )?;
        result
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let result = self.storage.size_key(key).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}size_key({key}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncListableStorageTraits> AsyncListableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        let keys = self.storage.list().await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}list() -> [{}]",
            (self.prefix_func)(),
            keys.as_ref().unwrap_or(&vec![]).iter().format(", "),
        )?;
        keys
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let result = self.storage.list_prefix(prefix).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}list_prefix({prefix}) -> [{}]",
            (self.prefix_func)(),
            result.as_ref().unwrap_or(&vec![]).iter().format(", ")
        )?;
        result
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let result = self.storage.list_dir(prefix).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}list_dir({prefix}) -> (keys:[{}], prefixes:[{}])",
            (self.prefix_func)(),
            result.as_ref().map_or(String::new(), |skp| skp
                .keys()
                .iter()
                .format(", ")
                .to_string()),
            result.as_ref().map_or(String::new(), |skp| skp
                .prefixes()
                .iter()
                .format(", ")
                .to_string()),
        )?;
        result
    }

    async fn size(&self) -> Result<u64, StorageError> {
        let result = self.storage.size().await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}size() -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let result: Result<u64, StorageError> = self.storage.size_prefix(prefix).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}size_prefix({prefix}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncWritableStorageTraits> AsyncWritableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    async fn set(&self, key: &StoreKey, value: AsyncBytes) -> Result<(), StorageError> {
        let len = value.len();
        let result = self.storage.set(key, value).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}set({key}, len={len}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }

    async fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        let result = self.storage.set_partial_values(key_start_values).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}set_partial_values({key_start_values:?}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        let result = self.storage.erase(key).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase({key}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }

    async fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        let result = self.storage.erase_values(keys).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase_values([{}]) -> {result:?}",
            (self.prefix_func)(),
            keys.iter().format(", ")
        )?;
        result
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        let result = self.storage.erase_prefix(prefix).await;
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase_prefix({prefix}) -> {result:?}",
            (self.prefix_func)()
        )?;
        result
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<TStorage: ?Sized + AsyncReadableWritableStorageTraits> AsyncReadableWritableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    // async fn mutex(&self, key: &StoreKey) -> Result<AsyncStoreKeyMutex, StorageError> {
    //     let result = self.storage.mutex(key).await;
    //     writeln!(
    //         self.handle.lock().unwrap(),
    //         "{}mutex({key}) -> {:?}",
    //         (self.prefix_func)(),
    //         result.as_ref().map_or((), |_| ())
    //     )?;
    //     result
    // }
}
