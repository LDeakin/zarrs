//! A storage transformer which prints function calls.

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use crate::{
    metadata::Metadata,
    storage::{
        store::{StoreKey, StoreKeys, StorePrefix},
        ListableStorage, ListableStorageTraits, ReadableStorage, ReadableStorageTraits,
        ReadableWritableStorage, ReadableWritableStorageTraits, StorageError, StoreKeyRange,
        StoreKeyStartValue, StoreKeysPrefixes, WritableStorage, WritableStorageTraits,
    },
};

use super::StorageTransformerExtension;

/// The usage log storage transformer.
#[derive()]
pub struct UsageLogStorageTransformer {
    prefix: String,
    handle: Arc<Mutex<dyn Write + Send + Sync>>,
}

impl core::fmt::Debug for UsageLogStorageTransformer {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        writeln!(f, "{}", self.prefix)
    }
}

impl UsageLogStorageTransformer {
    /// Create a new usage log storage transformer.
    pub fn new(prefix: &str, handle: Arc<Mutex<dyn Write + Send + Sync>>) -> Self {
        Self {
            prefix: prefix.to_string(),
            handle,
        }
    }

    fn create_transformer<TStorage: ?Sized>(
        &self,
        storage: Arc<TStorage>,
    ) -> Arc<UsageLogStorageTransformerImpl<TStorage>> {
        Arc::new(UsageLogStorageTransformerImpl {
            storage,
            prefix: self.prefix.clone(),
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
}

struct UsageLogStorageTransformerImpl<TStorage: ?Sized> {
    storage: Arc<TStorage>,
    prefix: String,
    handle: Arc<Mutex<dyn Write + Send + Sync>>,
}

impl<TStorage> core::fmt::Debug for UsageLogStorageTransformerImpl<TStorage> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        writeln!(f, "{}", self.prefix)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> ReadableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    fn get(&self, key: &StoreKey) -> Result<Vec<u8>, StorageError> {
        let result = self.storage.get(key);
        writeln!(
            self.handle.lock().unwrap(),
            "{}get({key:?}) -> len={:?}",
            self.prefix,
            result.as_ref().map(Vec::len)
        )?;
        result
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Vec<Result<Vec<u8>, StorageError>> {
        let _ = writeln!(
            self.handle.lock().unwrap(),
            "{}get({key_ranges:?})",
            self.prefix
        );
        self.storage.get_partial_values(key_ranges)
    }

    fn size(&self) -> Result<u64, StorageError> {
        self.storage.size()
    }

    fn size_key(&self, key: &StoreKey) -> Result<u64, StorageError> {
        self.storage.size_key(key)
    }
}

impl<TStorage: ?Sized + ListableStorageTraits> ListableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
    fn list(&self) -> Result<StoreKeys, StorageError> {
        writeln!(self.handle.lock().unwrap(), "{}list()", self.prefix)?;
        self.storage.list()
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}list_prefix({prefix:?})",
            self.prefix
        )?;
        self.storage.list_prefix(prefix)
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}list_dir({prefix:?})",
            self.prefix
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
            self.prefix,
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
            self.prefix
        )?;
        self.storage.set_partial_values(key_start_values)
    }

    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        writeln!(self.handle.lock().unwrap(), "{}erase({key:?}", self.prefix)?;
        self.storage.erase(key)
    }

    fn erase_values(&self, keys: &[StoreKey]) -> Result<(), StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase_values({keys:?}",
            self.prefix
        )?;
        self.storage.erase_values(keys)
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        writeln!(
            self.handle.lock().unwrap(),
            "{}erase_prefix({prefix:?}",
            self.prefix
        )?;
        self.storage.erase_prefix(prefix)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits + WritableStorageTraits> ReadableWritableStorageTraits
    for UsageLogStorageTransformerImpl<TStorage>
{
}
