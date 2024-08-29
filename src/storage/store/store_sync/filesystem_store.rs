//! A filesystem store.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html>.

use crate::{
    byte_range::{ByteOffset, ByteRange},
    storage::{
        store_set_partial_values, Bytes, ListableStorageTraits, ReadableStorageTraits,
        StorageError, StoreKey, StoreKeyError, StoreKeyStartValue, StoreKeys, StoreKeysPrefixes,
        StorePrefix, StorePrefixes, WritableStorageTraits,
    },
};

use parking_lot::RwLock;
use thiserror::Error;
use walkdir::WalkDir;

use std::{
    alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout},
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[cfg(target_os = "linux")]
use libc::O_DIRECT;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

// // Register the store.
// inventory::submit! {
//     ReadableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     WritableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     ListableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }
// inventory::submit! {
//     ReadableWritableStorePlugin::new("file", |uri| Ok(Arc::new(create_store_filesystem(uri)?)))
// }

// #[allow(clippy::similar_names)]
// fn create_store_filesystem(uri: &str) -> Result<FilesystemStore, StorePluginCreateError> {
//     let url = url::Url::parse(uri)?;
//     let path = std::path::PathBuf::from(url.path());
//     FilesystemStore::new(path).map_err(|e| StorePluginCreateError::Other(e.to_string()))
// }

/// For `O_DIRECT`, we need a buffer that is aligned to the page size and is a
/// multiple of the page size.
struct PageAlignedBuffer {
    buf: *mut u8,
    layout: Layout,
}

impl PageAlignedBuffer {
    /// Allocate a new page-size aligned buffer of `size` bytes. The actual size
    /// will be rounded up to the next largest multiple of the page size.
    pub fn new(size: usize) -> Self {
        let align = page_size::get();
        let layout = Layout::from_size_align(size, align)
            .expect("size and align are reasonable")
            .pad_to_align();

        assert!(layout.size() > 0);
        // SAFETY: `layout` is non-zero, as asserted above.
        let buf = unsafe { alloc_zeroed(layout) };

        // buf can be zero when out of memory, or if the allocator doesn't like
        // our `Layout`
        if buf.is_null() {
            handle_alloc_error(layout);
        }

        Self { buf, layout }
    }
}

impl Deref for PageAlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY:
        // * "data must be valid for reads for len * mem::size_of::<T>() many bytes, and it must be properly aligned"
        //      => T is u8 => alignment is trivial
        //      => `self.buf` is non-null, as per `buf.is_null` check above
        //      => `self.buf` is a single allocation
        // * "`data` must point to len consecutive properly initialized values of type T."
        //      => `self.buf` is zero-initialized
        // * "The memory referenced by the returned slice must not be mutated for the duration of lifetime 'a, except inside an UnsafeCell"
        //      => guaranteed by the borrow checker for us
        // * "The total size len * mem::size_of::<T>() of the slice must be no
        //    larger than isize::MAX, and adding that size to data must not “wrap
        //    around” the address space."
        //      => given from the invariants of `Layout`
        unsafe { std::slice::from_raw_parts(self.buf, self.layout.size()) }
    }
}

impl DerefMut for PageAlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: see `deref` with the following modification:
        // "The memory referenced by the returned slice must not be accessed
        // through any other pointer (not derived from the return value) for the
        // duration of lifetime 'a. Both read and write accesses are forbidden."
        //      => guaranteed by the mutable borrow
        unsafe { std::slice::from_raw_parts_mut(self.buf, self.layout.size()) }
    }
}

impl Drop for PageAlignedBuffer {
    fn drop(&mut self) {
        // SAFETY:
        // * "ptr must denote a block of memory currently allocated via this allocator,"
        //      => we get the pointer from `System.alloc_zeroed`, and it is only free'd here in `drop`
        // * "layout must be the same layout that was used to allocate that block of memory."
        //      => we use the `Layout` value previously used for allocation
        unsafe { dealloc(self.buf, self.layout) }
    }
}

/// Options for use with [`FilesystemStore`]
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct FilesystemStoreOptions {
    direct_io: bool,
}

impl FilesystemStoreOptions {
    /// Set whether or not to enable direct I/O. Needs support from the
    /// operating system (currently only Linux) and file system.
    pub fn direct_io(&mut self, direct_io: bool) -> &mut Self {
        self.direct_io = direct_io;
        self
    }
}

/// A synchronous file system store.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/stores/filesystem/v1.0.html>.
#[derive(Debug)]
pub struct FilesystemStore {
    base_path: PathBuf,
    sort: bool,
    readonly: bool,
    options: FilesystemStoreOptions,
    files: Mutex<HashMap<StoreKey, Arc<RwLock<()>>>>,
    // locks: StoreLocks,
}

impl FilesystemStore {
    /// Create a new file system store at a given `base_path`.
    ///
    /// # Errors
    /// Returns a [`FilesystemStoreCreateError`] if `base_directory`:
    ///   - is not valid, or
    ///   - it points to an existing file rather than a directory.
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, FilesystemStoreCreateError> {
        Self::new_with_options(base_path, FilesystemStoreOptions::default())
    }

    /// Create a new file system store at a given `base_path` and `options`.
    ///
    /// # Errors
    /// Returns a [`FilesystemStoreCreateError`] if `base_directory`:
    ///   - is not valid, or
    ///   - it points to an existing file rather than a directory.
    pub fn new_with_options<P: AsRef<Path>>(
        base_path: P,
        options: FilesystemStoreOptions,
    ) -> Result<Self, FilesystemStoreCreateError> {
        let base_path = base_path.as_ref().to_path_buf();
        if base_path.to_str().is_none() {
            return Err(FilesystemStoreCreateError::InvalidBasePath(base_path));
        }

        let readonly = if base_path.exists() {
            // the path already exists, check if it is read only
            let md = std::fs::metadata(&base_path).map_err(FilesystemStoreCreateError::IOError)?;
            md.permissions().readonly()
        } else {
            // the path does not exist, so try and create it. If this succeeds, the filesystem is not read only
            std::fs::create_dir_all(&base_path).map_err(FilesystemStoreCreateError::IOError)?;
            std::fs::remove_dir(&base_path)?;
            false
        };

        Ok(Self {
            base_path,
            sort: false,
            options,
            readonly,
            files: Mutex::default(),
        })
    }

    // /// Create a new file system store at a given `base_path` with non-default store locks.
    // ///
    // /// # Errors
    // /// Returns a [`FilesystemStoreCreateError`] if `base_directory`:
    // ///   - is not valid, or
    // ///   - it points to an existing file rather than a directory.
    // pub fn new_with_locks<P: AsRef<Path>>(
    //     base_path: P,
    //     store_locks: StoreLocks,
    // ) -> Result<Self, FilesystemStoreCreateError> {
    //     let base_path = base_path.as_ref().to_path_buf();
    //     if base_path.to_str().is_none() {
    //         return Err(FilesystemStoreCreateError::InvalidBasePath(base_path));
    //     }

    //     let readonly = if base_path.exists() {
    //         // the path already exists, check if it is read only
    //         let md = std::fs::metadata(&base_path).map_err(FilesystemStoreCreateError::IOError)?;
    //         md.permissions().readonly()
    //     } else {
    //         // the path does not exist, so try and create it. If this succeeds, the filesystem is not read only
    //         std::fs::create_dir_all(&base_path).map_err(FilesystemStoreCreateError::IOError)?;
    //         std::fs::remove_dir(&base_path)?;
    //         false
    //     };

    //     Ok(Self {
    //         base_path,
    //         sort: false,
    //         readonly,
    //         files: Mutex::default(),
    //         locks: store_locks,
    //     })
    // }

    /// Makes the store sort directories/files when walking.
    #[must_use]
    pub const fn sorted(mut self) -> Self {
        self.sort = true;
        self
    }

    /// Maps a [`StoreKey`] to a filesystem [`PathBuf`].
    #[must_use]
    pub fn key_to_fspath(&self, key: &StoreKey) -> PathBuf {
        let mut path = self.base_path.clone();
        if !key.as_str().is_empty() {
            path.push(key.as_str().strip_prefix('/').unwrap_or(key.as_str()));
        }
        path
    }

    /// Maps a filesystem [`PathBuf`] to a [`StoreKey`].
    fn fspath_to_key(&self, path: &std::path::Path) -> Result<StoreKey, StoreKeyError> {
        let path = pathdiff::diff_paths(path, &self.base_path)
            .ok_or_else(|| StoreKeyError::from(path.to_str().unwrap_or_default().to_string()))?;
        let path_str = path.to_string_lossy();
        #[cfg(target_os = "windows")]
        {
            StoreKey::new(path_str.replace("\\", "/"))
        }
        #[cfg(not(target_os = "windows"))]
        {
            StoreKey::new(path_str)
        }
    }

    /// Maps a store [`StorePrefix`] to a filesystem [`PathBuf`].
    #[must_use]
    pub fn prefix_to_fs_path(&self, prefix: &StorePrefix) -> PathBuf {
        let mut path = self.base_path.clone();
        path.push(prefix.as_str());
        path
    }

    fn get_file_mutex(&self, key: &StoreKey) -> Arc<RwLock<()>> {
        let mut files = self.files.lock().unwrap();
        let file = files
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RwLock::default()))
            .clone();
        drop(files);
        file
    }

    fn set_impl(
        &self,
        key: &StoreKey,
        value: &[u8],
        offset: Option<ByteOffset>,
        truncate: bool,
    ) -> Result<(), StorageError> {
        let file = self.get_file_mutex(key);
        let _lock = file.write();

        // Create directories
        let key_path = self.key_to_fspath(key);
        if let Some(parent) = key_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut flags = OpenOptions::new();
        flags.write(true).create(true).truncate(truncate);

        // FIXME: for now, only Linux support; also no support for `offset != 0`
        let enable_direct = cfg!(target_os = "linux")
            && self.options.direct_io
            && offset.is_none()
            && !value.is_empty();

        #[cfg(target_os = "linux")]
        if enable_direct {
            flags.custom_flags(O_DIRECT);
        }

        let mut file = flags.open(key_path)?;

        // Write
        if enable_direct {
            let mut buf = PageAlignedBuffer::new(value.len());
            buf[0..value.len()].copy_from_slice(value);
            file.write_all(&buf)?;
            file.set_len(value.len() as u64)?;
        } else {
            if let Some(offset) = offset {
                file.seek(SeekFrom::Start(offset))?;
            }

            file.write_all(value)?;
        }

        Ok(())
    }
}

impl ReadableStorageTraits for FilesystemStore {
    fn get_partial_values_key(
        &self,
        key: &StoreKey,
        byte_ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, StorageError> {
        let file = self.get_file_mutex(key);
        let _lock = file.read();

        let mut file = match File::open(self.key_to_fspath(key)) {
            Ok(file) => file,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                }
                return Err(err.into());
            }
        };

        let mut out = Vec::with_capacity(byte_ranges.len());
        for byte_range in byte_ranges {
            let bytes = {
                // Seek
                match byte_range {
                    ByteRange::FromStart(offset, _) => file.seek(SeekFrom::Start(*offset)),
                    ByteRange::FromEnd(_, None) => file.seek(SeekFrom::Start(0u64)),
                    ByteRange::FromEnd(offset, Some(length)) => {
                        file.seek(SeekFrom::End(-(i64::try_from(*offset + *length).unwrap())))
                    }
                }?;

                // Read
                match byte_range {
                    ByteRange::FromStart(_, None) | ByteRange::FromEnd(_, None) => {
                        let mut buffer = Vec::new();
                        file.read_to_end(&mut buffer)?;
                        buffer
                    }
                    ByteRange::FromStart(_, Some(length)) | ByteRange::FromEnd(_, Some(length)) => {
                        let length = usize::try_from(*length).unwrap();
                        let mut buffer = vec![0; length];
                        file.read_exact(&mut buffer)?;
                        buffer
                    }
                }
            };
            out.push(Bytes::from(bytes));
        }

        Ok(Some(out))
    }

    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let key_path = self.key_to_fspath(key);
        std::fs::metadata(key_path).map_or_else(|_| Ok(None), |metadata| Ok(Some(metadata.len())))
    }
}

impl WritableStorageTraits for FilesystemStore {
    fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        if self.readonly {
            Err(StorageError::ReadOnly)
        } else {
            Self::set_impl(self, key, &value, None, true)
        }
    }

    fn set_partial_values(
        &self,
        key_start_values: &[StoreKeyStartValue],
    ) -> Result<(), StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        store_set_partial_values(self, key_start_values)
    }

    fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        let file = self.get_file_mutex(key);
        let _lock = file.write();

        let key_path = self.key_to_fspath(key);
        let result = std::fs::remove_file(key_path);
        if let Err(err) = result {
            match err.kind() {
                std::io::ErrorKind::NotFound => Ok(()),
                _ => Err(err.into()),
            }
        } else {
            Ok(())
        }
    }

    fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        if self.readonly {
            return Err(StorageError::ReadOnly);
        }

        let _lock = self.files.lock(); // lock all operations

        let prefix_path = self.prefix_to_fs_path(prefix);
        let result = std::fs::remove_dir_all(prefix_path);
        if let Err(err) = result {
            match err.kind() {
                std::io::ErrorKind::NotFound => Ok(()),
                _ => Err(err.into()),
            }
        } else {
            Ok(())
        }
    }
}

impl ListableStorageTraits for FilesystemStore {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        Ok(WalkDir::new(&self.base_path)
            .sort_by_file_name()
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|v| v.path().is_file())
            .filter_map(|v| self.fspath_to_key(v.path()).ok())
            .collect())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        Ok(WalkDir::new(self.prefix_to_fs_path(prefix))
            .sort_by_file_name()
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter(|v| v.path().is_file())
            .filter_map(|v| self.fspath_to_key(v.path()).ok())
            .collect())
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let prefix_path = self.prefix_to_fs_path(prefix);
        let mut keys: StoreKeys = vec![];
        let mut prefixes: StorePrefixes = vec![];
        let dir = std::fs::read_dir(prefix_path);
        if let Ok(dir) = dir {
            for entry in dir {
                let entry = entry?;
                let fs_path = entry.path();
                let path = fs_path.file_name().unwrap();
                if fs_path.is_dir() {
                    prefixes.push(StorePrefix::new(
                        prefix.as_str().to_string() + path.to_str().unwrap() + "/",
                    )?);
                } else {
                    keys.push(StoreKey::new(
                        prefix.as_str().to_owned() + path.to_str().unwrap(),
                    )?);
                }
            }
        }
        if self.sort {
            keys.sort();
            prefixes.sort();
        }

        Ok(StoreKeysPrefixes { keys, prefixes })
    }

    fn size(&self) -> Result<u64, StorageError> {
        Ok(WalkDir::new(&self.base_path)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .filter_map(|v| {
                if v.path().is_file() {
                    Some(std::fs::metadata(v.path()).unwrap().len())
                } else {
                    None
                }
            })
            .sum())
    }

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let mut size = 0;
        for key in self.list_prefix(prefix)? {
            if let Some(size_key) = self.size_key(&key)? {
                size += size_key;
            }
        }
        Ok(size)
    }
}

/// A filesystem store creation error.
#[derive(Debug, Error)]
pub enum FilesystemStoreCreateError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// The path is not valid on this system.
    #[error("base path {0} is not valid")]
    InvalidBasePath(PathBuf),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn filesystem() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let store = FilesystemStore::new(path.path())?.sorted();
        super::super::test_util::store_write(&store)?;
        super::super::test_util::store_read(&store)?;
        super::super::test_util::store_list(&store)?;
        Ok(())
    }

    #[test]
    // #[cfg_attr(miri, ignore)]
    fn direct_io() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let mut opts = FilesystemStoreOptions::default();
        opts.direct_io(true);

        let store = FilesystemStore::new_with_options(path.path(), opts)?.sorted();
        super::super::test_util::store_write(&store)?;
        super::super::test_util::store_read(&store)?;
        super::super::test_util::store_list(&store)?;
        Ok(())
    }
}
