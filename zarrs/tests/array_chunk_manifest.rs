use std::sync::Arc;

use zarrs::array::Array;
use zarrs_filesystem::FilesystemStore;
use zarrs_storage::ReadableListableStorage;

#[cfg(feature = "transpose")]
#[test]
fn array_chunk_manifest() {
    let store: ReadableListableStorage =
        Arc::new(FilesystemStore::new("tests/data/virtualizarr").unwrap());
    let array = Array::open(store.clone(), "/virtualizarr.zarr/data").unwrap();

    assert_eq!(
        array.retrieve_chunk_elements::<i64>(&[0, 0]).unwrap(),
        (0..16).collect::<Vec<_>>()
    );
    assert_eq!(
        array.retrieve_chunk_elements::<i64>(&[1, 0]).unwrap(),
        (16..32).collect::<Vec<_>>()
    );
    assert_eq!(
        array.retrieve_chunk_elements::<i64>(&[0, 1]).unwrap(),
        vec![0; 2 * 8]
    );
}
