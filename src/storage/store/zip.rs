//! A zip store.

use crate::{
    byte_range::ByteRange,
    storage::{
        ListableStorageTraits, ReadableStorageTraits, StorageError, StoreKeyRange,
        StoreKeysPrefixes,
    },
};

use super::{
    ListableStoreExtension, ReadableStoreExtension, StoreExtension, StoreKey, StoreKeys,
    StorePrefix, StorePrefixes,
};

use itertools::Itertools;
use thiserror::Error;
use zip::{result::ZipError, ZipArchive};

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

// // Register the store.
// inventory::submit! {
//     ReadableStorePlugin::new("zip", |uri| Ok(Arc::new(create_store_zip(uri)?)))
// }
// inventory::submit! {
//     WritableStorePlugin::new("zip", |uri| Ok(Arc::new(create_store_zip(uri)?)))
// }
// inventory::submit! {
//     ListableStorePlugin::new("zip", |uri| Ok(Arc::new(create_store_zip(uri)?)))
// }
// inventory::submit! {
//     ReadableWritableStorePlugin::new("zip", |uri| Ok(Arc::new(create_store_zip(uri)?)))
// }

// #[allow(clippy::similar_names)]
// fn create_store_zip(uri: &str) -> Result<ZipStore, StorePluginCreateError> {
//     let url = url::Url::parse(uri)?;
//     let path = std::path::PathBuf::from(url.path());
//     ZipStore::new(path).map_err(|e| StorePluginCreateError::Other(e.to_string()))
// }

/// A zip store.
///
/// See <https://zarr-specs.readthedocs.io/en/latest/v3/stores/zip/v1.0.html>.
pub struct ZipStore {
    path: PathBuf,
    zip_archive: Arc<Mutex<ZipArchive<File>>>,
    size: usize,
}

impl std::fmt::Debug for ZipStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.path.fmt(f)
    }
}

impl ReadableStoreExtension for ZipStore {}

impl ListableStoreExtension for ZipStore {}

impl StoreExtension for ZipStore {
    fn uri_scheme(&self) -> Option<&'static str> {
        Some("zip")
    }
}

impl ZipStore {
    /// Create a new zip store for the zip file at `zip_path`.
    ///
    /// # Errors
    ///
    /// Returns a [`ZipStoreCreateError`] if `zip_path` is not valid zip file.
    pub fn new<P: AsRef<Path>>(zip_path: P) -> Result<ZipStore, ZipStoreCreateError> {
        let path = zip_path.as_ref().to_path_buf();
        if path.is_dir() {
            Err(ZipStoreCreateError::ExistingDir(path))
        } else {
            let zip_file = File::open(&path)?;
            let size = usize::try_from(zip_file.metadata()?.len())
                .map_err(|_| ZipError::UnsupportedArchive("zip file is too large"))?;
            let zip_archive = Arc::new(Mutex::new(ZipArchive::new(zip_file)?));
            Ok(ZipStore {
                path,
                zip_archive,
                size,
            })
        }
    }

    fn get_impl(&self, key: &StoreKey, byte_range: &ByteRange) -> Result<Vec<u8>, StorageError> {
        let mut zip_archive = self.zip_archive.lock().unwrap();
        let file = zip_archive
            .by_name(key.as_str())
            .map_err(|err| StorageError::Other(err.to_string()))?;
        let size = usize::try_from(file.size()).map_err(|_| {
            StorageError::Other("zip archive internal file larger than usize".to_string())
        })?;
        let bytes = file.bytes();

        let buffer = match byte_range {
            ByteRange::FromStart(offset, None) => {
                bytes.skip(*offset).collect::<Result<Vec<_>, _>>()?
            }
            ByteRange::FromStart(offset, Some(length)) => bytes
                .skip(*offset)
                .take(*length)
                .collect::<Result<Vec<_>, _>>()?,
            ByteRange::FromEnd(offset, None) => {
                bytes.take(size - offset).collect::<Result<Vec<_>, _>>()?
            }
            ByteRange::FromEnd(offset, Some(length)) => bytes
                .skip(size - length - offset)
                .take(*length)
                .collect::<Result<Vec<_>, _>>()?,
        };

        Ok(buffer)
    }
}

impl ReadableStorageTraits for ZipStore {
    fn get(&self, key: &StoreKey) -> Result<Vec<u8>, StorageError> {
        self.get_impl(key, &ByteRange::FromStart(0, None))
    }

    fn get_partial_values(
        &self,
        key_ranges: &[StoreKeyRange],
    ) -> Vec<Result<Vec<u8>, StorageError>> {
        let mut out = Vec::with_capacity(key_ranges.len());
        for key_range in key_ranges {
            out.push(self.get_impl(&key_range.key, &key_range.byte_range));
        }
        out
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl ListableStorageTraits for ZipStore {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        let zip_archive = self.zip_archive.lock().unwrap();
        Ok(zip_archive
            .file_names()
            .filter_map(|v| StoreKey::try_from(v).ok())
            .sorted()
            .collect())
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let mut zip_archive = self.zip_archive.lock().unwrap();
        let file_names: Vec<String> = zip_archive
            .file_names()
            .map(std::string::ToString::to_string)
            .collect();
        Ok(file_names
            .into_iter()
            .filter_map(|name| {
                if name.starts_with(prefix.as_str()) {
                    if let Ok(file) = zip_archive.by_name(&name) {
                        if file.is_file() {
                            let name = name.strip_suffix('/').unwrap_or(&name);
                            if let Ok(store_key) = StoreKey::try_from(name) {
                                return Some(store_key);
                            }
                        }
                    }
                }
                None
            })
            .sorted()
            .collect())
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let mut zip_archive = self.zip_archive.lock().unwrap();
        let mut keys: StoreKeys = vec![];
        let mut prefixes: StorePrefixes = vec![];
        let file_names: Vec<String> = zip_archive
            .file_names()
            .map(std::string::ToString::to_string)
            .collect();
        for name in file_names {
            if name.starts_with(prefix.as_str()) {
                if let Ok(file) = zip_archive.by_name(&name) {
                    if file.is_file() {
                        let name = name.strip_suffix('/').unwrap_or(&name);
                        if let Ok(store_key) = StoreKey::try_from(name) {
                            keys.push(store_key);
                        }
                    } else if file.is_dir() {
                        if let Ok(store_prefix) = StorePrefix::try_from(name.as_str()) {
                            prefixes.push(store_prefix);
                        }
                    }
                }
            }
        }
        keys.sort();
        prefixes.sort();

        Ok(StoreKeysPrefixes { keys, prefixes })
    }
}

/// A zip store creation error.
#[derive(Debug, Error)]
pub enum ZipStoreCreateError {
    /// An IO error.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    /// An existing directory.
    #[error("{0} is an existing directory, not a zip file")]
    ExistingDir(PathBuf),
    /// A zip error.
    #[error(transparent)]
    ZipError(#[from] ZipError),
}

#[cfg(test)]
mod tests {
    use walkdir::WalkDir;

    use crate::storage::{store::FilesystemStore, WritableStorageTraits};

    use super::*;
    use std::{error::Error, io::Write};

    // https://github.com/zip-rs/zip/blob/master/examples/write_dir.rs
    fn zip_dir(
        it: &mut dyn Iterator<Item = walkdir::DirEntry>,
        prefix: &str,
        writer: File,
        method: zip::CompressionMethod,
    ) -> zip::result::ZipResult<()> {
        let mut zip = zip::ZipWriter::new(writer);
        let options = zip::write::FileOptions::default().compression_method(method);
        let mut buffer = Vec::new();
        for entry in it {
            let path = entry.path();
            let name = path.strip_prefix(Path::new(prefix)).unwrap();
            if path.is_file() {
                #[allow(deprecated)]
                zip.start_file_from_path(name, options)?;
                let mut f = File::open(path)?;
                f.read_to_end(&mut buffer)?;
                zip.write_all(&buffer)?;
                buffer.clear();
            } else if !name.as_os_str().is_empty() {
                #[allow(deprecated)]
                zip.add_directory_from_path(name, options)?;
            }
        }
        zip.finish()?;
        Result::Ok(())
    }

    fn zip_write(path: &Path) -> Result<(), Box<dyn Error>> {
        let tmp_path = tempfile::TempDir::new()?;
        let tmp_path = tmp_path.path();
        let store = FilesystemStore::new(tmp_path)?.sorted();
        store.set(&"a/b".try_into()?, &[0, 1, 2, 3])?;
        store.set(&"a/c".try_into()?, &[])?;
        store.set(&"a/d/e".try_into()?, &[])?;
        store.set(&"a/f/g".try_into()?, &[])?;
        store.set(&"a/f/h".try_into()?, &[])?;
        store.set(&"b/c/d".try_into()?, &[])?;

        let walkdir = WalkDir::new(tmp_path);

        let file = File::create(path).unwrap();
        zip_dir(
            &mut walkdir.into_iter().filter_map(|e| e.ok()),
            tmp_path.to_str().unwrap(),
            file,
            zip::CompressionMethod::Stored,
        )?;

        Ok(())
    }

    #[test]
    fn zip_list() -> Result<(), Box<dyn Error>> {
        let path = tempfile::TempDir::new()?;
        let mut path = path.path().to_path_buf();
        path.push("test.zip");
        zip_write(&path).unwrap();

        println!("{path:?}");

        let store = ZipStore::new(path)?;

        assert_eq!(
            store.list()?,
            &[
                "a/b".try_into()?,
                "a/c".try_into()?,
                "a/d/e".try_into()?,
                "a/f/g".try_into()?,
                "a/f/h".try_into()?,
                "b/c/d".try_into()?
            ]
        );
        assert_eq!(
            store.list_prefix(&"a/".try_into()?)?,
            &[
                "a/b".try_into()?,
                "a/c".try_into()?,
                "a/d/e".try_into()?,
                "a/f/g".try_into()?,
                "a/f/h".try_into()?,
            ]
        );
        assert_eq!(
            store.list_prefix(&"a/d/".try_into()?)?,
            &["a/d/e".try_into()?]
        );
        assert_eq!(
            store.list_prefix(&"".try_into()?)?,
            &[
                "a/b".try_into()?,
                "a/c".try_into()?,
                "a/d/e".try_into()?,
                "a/f/g".try_into()?,
                "a/f/h".try_into()?,
                "b/c/d".try_into()?
            ]
        );

        let list = store.list_dir(&"".try_into()?)?;
        assert_eq!(
            list.keys(),
            &[
                "a/b".try_into()?,
                "a/c".try_into()?,
                "a/d/e".try_into()?,
                "a/f/g".try_into()?,
                "a/f/h".try_into()?,
                "b/c/d".try_into()?
            ]
        );
        assert_eq!(
            list.prefixes(),
            &[
                "a/".try_into()?,
                "a/d/".try_into()?,
                "a/f/".try_into()?,
                "b/".try_into()?,
                "b/c/".try_into()?
            ]
        );

        assert!(crate::storage::node_exists(&store, &"/a/b".try_into()?)?);
        assert!(crate::storage::node_exists_listable(
            &store,
            &"/a/b".try_into()?
        )?);

        assert_eq!(store.get(&"a/b".try_into()?)?, &[0, 1, 2, 3]);
        assert_eq!(store.get(&"a/c".try_into()?)?, Vec::<u8>::new().as_slice());

        Ok(())
    }
}
