//! A HTTP store.

use std::sync::Arc;

use crate::{
    object_store_impl,
    storage::{
        store_lock::{AsyncDefaultStoreLocks, AsyncStoreLocks},
        StorageError,
    },
};

use object_store::http::{HttpBuilder, HttpStore};

/// A HTTP store.
#[derive(Debug)]
pub struct AsyncHTTPStore {
    object_store: HttpStore,
    locks: AsyncStoreLocks,
}

impl AsyncHTTPStore {
    /// Create a new HTTP store at a given `base_url`.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if `base_url` is not valid.
    pub fn new(base_url: &str) -> Result<Self, StorageError> {
        Self::new_with_locks(base_url, Arc::new(AsyncDefaultStoreLocks::default()))
    }

    /// Create a new HTTP store at a given `base_url` with non-default store locks.
    ///
    /// # Errors
    ///
    /// Returns a [`StorageError`] if `base_url` is not valid.
    pub fn new_with_locks(
        base_url: &str,
        store_locks: AsyncStoreLocks,
    ) -> Result<Self, StorageError> {
        let object_store = HttpBuilder::new().with_url(base_url).build()?;
        Ok(Self {
            object_store,
            locks: store_locks,
        })
    }
}

object_store_impl!(AsyncHTTPStore, object_store, locks);

#[cfg(test)]
mod tests {
    use crate::{
        array::{Array, DataType},
        node::NodePath,
        storage::{meta_key, AsyncReadableStorageTraits},
    };

    use super::*;

    const HTTP_TEST_PATH_REF: &str =
        "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/hierarchy.zarr";
    const ARRAY_PATH_REF: &str = "/a/baz";

    #[tokio::test]
    async fn http_store_size() {
        let store = AsyncHTTPStore::new(HTTP_TEST_PATH_REF).unwrap();
        let len = store
            .size_key(&meta_key(&NodePath::new(ARRAY_PATH_REF).unwrap()))
            .await
            .unwrap();
        assert_eq!(len.unwrap(), 691);
    }

    #[tokio::test]
    async fn http_store_get() {
        let store = AsyncHTTPStore::new(HTTP_TEST_PATH_REF).unwrap();
        let metadata = store
            .get(&meta_key(&NodePath::new(ARRAY_PATH_REF).unwrap()))
            .await
            .unwrap()
            .unwrap();
        let metadata: crate::array::ArrayMetadataV3 = serde_json::from_slice(&metadata).unwrap();
        assert_eq!(metadata.data_type.name(), "float64");
    }

    #[tokio::test]
    async fn http_store_array() {
        let store = AsyncHTTPStore::new(HTTP_TEST_PATH_REF).unwrap();
        let array = Array::async_new(store.into(), ARRAY_PATH_REF)
            .await
            .unwrap();
        assert_eq!(array.data_type(), &DataType::Float64);
    }

    #[cfg(feature = "gzip")]
    #[tokio::test]
    async fn http_store_array_get() {
        const HTTP_TEST_PATH: &str =
            "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/array_write_read.zarr";
        const ARRAY_PATH: &str = "/group/array";

        let store = AsyncHTTPStore::new(HTTP_TEST_PATH).unwrap();
        let array = Array::async_new(store.into(), ARRAY_PATH).await.unwrap();
        assert_eq!(array.data_type(), &DataType::Float32);

        // Read the central 4x2 subset of the array
        let subset_4x2 =
            crate::array_subset::ArraySubset::new_with_start_shape(vec![2, 3], vec![4, 2]).unwrap(); // the center 4x2 region
        let data_4x2 = array
            .async_retrieve_array_subset_elements::<f32>(&subset_4x2)
            .await
            .unwrap();
        // assert_eq!(data_4x2, &[0.0, f32::NAN, 0.1, f32::NAN, 0.4, 0.5, 0.7, 0.8]);
        assert_eq!(data_4x2[0], 0.0);
        assert!(data_4x2[1].is_nan());
        assert_eq!(data_4x2[2], 0.1);
        assert!(data_4x2[3].is_nan());
        assert_eq!(data_4x2[4], 0.4);
        assert_eq!(data_4x2[5], 0.5);
        assert_eq!(data_4x2[6], 0.7);
        assert_eq!(data_4x2[7], 0.8);

        // let data = array.retrieve_array_subset_ndarray::<f32>(&ArraySubset::new_with_shape(array.shape().to_vec())).unwrap();
        // println!("{data:?}");
    }

    #[cfg(all(feature = "sharding", feature = "gzip", feature = "crc32c"))]
    #[tokio::test]
    async fn http_store_sharded_array_get() {
        const HTTP_TEST_PATH_SHARDED: &str =
            "https://raw.githubusercontent.com/LDeakin/zarrs/main/tests/data/sharded_array_write_read.zarr";
        const ARRAY_PATH_SHARDED: &str = "/group/array";

        let store = AsyncHTTPStore::new(HTTP_TEST_PATH_SHARDED).unwrap();
        let array = Array::async_new(store.into(), ARRAY_PATH_SHARDED)
            .await
            .unwrap();
        assert_eq!(array.data_type(), &DataType::UInt16);

        // Read the central 4x2 subset of the array
        let subset_4x2 =
            crate::array_subset::ArraySubset::new_with_start_shape(vec![2, 3], vec![4, 2]).unwrap(); // the center 4x2 region
        let data_4x2 = array
            .async_retrieve_array_subset_elements::<u16>(&subset_4x2)
            .await
            .unwrap();
        assert_eq!(data_4x2, [19, 20, 27, 28, 35, 36, 43, 44].into());

        // let data = array.retrieve_array_subset_ndarray::<u16>(&ArraySubset::new_with_shape(array.shape().to_vec())).unwrap();
        // println!("{data:?}");
    }
}
