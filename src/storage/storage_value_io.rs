use std::{
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

use crate::byte_range::ByteRange;

use super::{ReadableStorageTraits, StorageError, StoreKey, StoreKeyRange};

/// Provides a [`Read`] interface to a storage value.
#[derive(Clone)]
pub struct StorageValueIO<TStorage: ?Sized + ReadableStorageTraits> {
    storage: Arc<TStorage>,
    key: StoreKey,
    pos: u64,
    size: u64,
}

impl<TStorage: ?Sized + ReadableStorageTraits> StorageValueIO<TStorage> {
    /// Create a new `StorageValueIO` for the `key` in `storage`.
    ///
    /// # Errors
    ///
    /// Returns a `StorageError` if the the size of the value at key cannot be determined.
    pub fn new(storage: Arc<TStorage>, key: StoreKey) -> Result<Self, StorageError> {
        let size = storage.size_key(&key)?;
        Ok(Self {
            storage,
            key,
            pos: 0,
            size,
        })
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> Seek for StorageValueIO<TStorage> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        use std::io::{Error, ErrorKind};
        self.pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => {
                let pos = i64::try_from(self.pos)
                    .map_err(|_| Error::from(ErrorKind::InvalidInput))?
                    + offset;
                u64::try_from(pos).map_err(|_| Error::from(ErrorKind::InvalidInput))?
            }
            SeekFrom::End(offset) => {
                let pos = i64::try_from(self.size)
                    .map_err(|_| Error::from(ErrorKind::InvalidInput))?
                    + offset;
                u64::try_from(pos).map_err(|_| Error::from(ErrorKind::InvalidInput))?
            }
        };
        Ok(self.pos)
    }
}

impl<TStorage: ?Sized + ReadableStorageTraits> Read for StorageValueIO<TStorage> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = buf.len() as u64;
        let key_range =
            StoreKeyRange::new(self.key.clone(), ByteRange::FromStart(self.pos, Some(len)));
        let data = self
            .storage
            .get_partial_values(&[key_range])
            .remove(0)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;
        buf.copy_from_slice(&data);
        self.pos += data.len() as u64;
        Ok(data.len())
    }
}
