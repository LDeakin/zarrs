#![allow(missing_docs)]

use std::{error::Error, path::PathBuf, sync::Arc};

use zarrs::{array::Array, array_subset::ArraySubset, storage::StoreKey};
use zarrs_filesystem::FilesystemStore;
use zarrs_zip::ZipStorageAdapter;

#[test]
fn zarr_python_compat_zip_store() -> Result<(), Box<dyn Error>> {
    let path = PathBuf::from("tests/data/zarr_python_compat");
    let store = Arc::new(FilesystemStore::new(&path)?);
    let store = Arc::new(ZipStorageAdapter::new(store, StoreKey::new("zarr.zip")?)?);

    let array = Array::open(store, "/foo")?;
    assert_eq!(array.shape(), vec![100, 100]);
    let elements = array.retrieve_array_subset_elements::<u8>(&ArraySubset::new_with_shape(
        array.shape().to_vec(),
    ))?;
    assert_eq!(elements, vec![42u8; 100 * 100]);

    Ok(())
}

#[cfg(feature = "fletcher32")]
#[test]
fn zarr_python_compat_fletcher32_v2() -> Result<(), Box<dyn Error>> {
    // NOTE: could support numcodecs.zarr3.fletcher32, but would need to permit and ignore "id" field
    // zarrs::config::global_config_mut()
    //     .experimental_codec_names_mut()
    //     .entry("fletcher32".to_string())
    //     .and_modify(|e| *e = "numcodecs.fletcher32".to_string());

    let path = PathBuf::from("tests/data/zarr_python_compat/fletcher32.zarr");
    let store = Arc::new(FilesystemStore::new(&path)?);

    let array = Array::open(store, "/")?;
    assert_eq!(array.shape(), vec![100, 100]);
    let elements = array.retrieve_array_subset_elements::<u16>(&ArraySubset::new_with_shape(
        array.shape().to_vec(),
    ))?;
    assert_eq!(elements, (0..100 * 100).collect::<Vec<u16>>());

    Ok(())
}
