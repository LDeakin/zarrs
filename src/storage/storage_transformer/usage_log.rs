//! A storage transformer which prints function calls.

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use crate::{
    array::MaybeBytes,
    byte_range::ByteRange,
    metadata::Metadata,
    storage::{
        store::{StoreKey, StoreKeys, StorePrefix},
        ListableStorage, ListableStorageTraits, ReadableListableStorage,
        ReadableListableStorageTraits, ReadableStorage, ReadableStorageTraits,
        ReadableWritableStorage, ReadableWritableStorageTraits, StorageError, StoreKeyRange,
        StoreKeyStartValue, StoreKeysPrefixes, WritableStorage, WritableStorageTraits,
    },
};

use super::StorageTransformerExtension;

/// The usage log storage transformer.
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
    fn create_metadata(&self) -> Option<Metadata> {
        None
    }

    fn create_readable_transformer<'a>(&self, storage: ReadableStorage<'a>) -> ReadableStorage<'a> {
        self.create_transformer(storage)
    }

    fn create_writable_transformer<'a>(&self, storage: WritableStorage<'a>) -> WritableStorage<'a> {
        self.create_transformer(storage)
    }

    fn create_listable_transformer<'a>(&self, storage: ListableStorage<'a>) -> ListableStorage<'a> {
        self.create_transformer(storage)
    }

    fn create_readable_writable_transformer<'a>(
        &self,
        storage: ReadableWritableStorage<'a>,
    ) -> ReadableWritableStorage<'a> {
        self.create_transformer(storage)
    }

    fn create_readable_listable_transformer<'a>(
        &self,
        storage: ReadableListableStorage<'a>,
    ) -> ReadableListableStorage<'a> {
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
            "{}get({key:?}) -> len={:?}",
            (self.prefix_func)(),
            result
                .as_ref()
                .map(|v| v.as_ref().map_or(0, std::vec::Vec::len))
        )?;
        result
    }

    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, StorageError> {
        let _ = writeln!(
            self.handle.lock().unwrap(),
            "{}get_partial_values_key({key}, {byte_ranges:?})",
            (self.prefix_func)()
        );
        self.storage.get_partial_values_key(key, byte_ranges)
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Result<Vec<MaybeBytes>, StorageError> {
        let _ = writeln!(
            self.handle.lock().unwrap(),
            "{}get_partial_values({key_ranges:?})",
            (self.prefix_func)()
        );
        self.storage.get_partial_values(key_ranges)
    }

    fn size(&self) -> Result<u64, StorageError> {
        let size = self.storage.size();
        let _ = writeln!(
            self.handle.lock().unwrap(),
            "{}size() -> {size:?}",
            (self.prefix_func)()
        );
        size
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let size = self.storage.size_key(key);
        let _ = writeln!(
            self.handle.lock().unwrap(),
            "{}size_key({key}) -> {size:?}",
            (self.prefix_func)()
        );
        size
    }
}

impl<TStorage: ?Sized + ListableStorageTraits> ListableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    fn list(&self) -> Result<StoreKeys, StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}list()",
            (self.prefix_func)()
        )?;
        self.storage.list()
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}list_prefix({prefix:?})",
            (self.prefix_func)()
        )?;
        self.storage.list_prefix(prefix)
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}list_dir({prefix:?})",
            (self.prefix_func)()
        )?;
        self.storage.list_dir(prefix)
    }
}

impl<TStorage: ?Sized + WritableStorageTraits> WritableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    fn set(&self, key: &StoreKey, value: &[u8]) -> Result<(), StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}set({key:?}, len={})",
            (self.prefix_func)(),
            value.len()
        )?;
        self.storage.set(key, value)
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}set_partial_values({key_start_values:?}",
            (self.prefix_func)()
        )?;
        self.storage.set_partial_values(key_start_values)
    }

    fn erase(&self, key: &StoreKey) -> Result<bool, StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase({key:?}",
            (self.prefix_func)()
        )?;
        self.storage.erase(key)
    }

    fn erase_values(&self, keys: &[StoreKey]) -> Result<bool, StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase_values({keys:?}",
            (self.prefix_func)()
        )?;
        self.storage.erase_values(keys)
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<bool, StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase_prefix({prefix:?}",
            (self.prefix_func)()
        )?;
        self.storage.erase_prefix(prefix)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits + WritableStorageTraits> ReadableWritableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
}

impl<TStorage: ?Sized + ReadableStorageTraits + ListableStorageTraits> ReadableListableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
}
