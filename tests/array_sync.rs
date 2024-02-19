use std::sync::Arc;

use zarrs::array::codec::{array_to_bytes::sharding::ShardingCodecBuilder, GzipCodec};
use zarrs::array::{Array, ArrayBuilder, ArrayView, DataType, FillValue};
use zarrs::array_subset::ArraySubset;
use zarrs::storage::store::MemoryStore;

#[cfg(feature = "ndarray")]
#[rustfmt::skip]
fn array_sync_read(array: Array<MemoryStore>) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(array.data_type(), &DataType::UInt8);
    assert_eq!(array.fill_value().as_ne_bytes(), &[0u8]);
    assert_eq!(array.shape(), &[4, 4]);
    assert_eq!(array.chunk_shape(&[0, 0]).unwrap(), [2, 2].try_into().unwrap());
    assert_eq!(array.chunk_grid_shape().unwrap(), &[2, 2]);

    // 1  2 | 3  4 
    // 5  6 | 7  8
    // -----|-----
    // 9 10 | 0  0
    // 0  0 | 0  0
    array.store_chunk(&[0, 0], vec![1, 2, 0, 0])?;
    array.store_chunk(&[0, 1], vec![3, 4, 7, 8])?;
    array.store_array_subset(&ArraySubset::new_with_ranges(&[1..3, 0..2]), vec![5, 6, 9, 10])?;

    assert!(array.retrieve_chunk(&[0, 0, 0]).is_err());
    assert_eq!(array.retrieve_chunk(&[0, 0])?, [1, 2, 5, 6]);
    assert_eq!(array.retrieve_chunk(&[0, 1])?, [3, 4, 7, 8]);
    assert_eq!(array.retrieve_chunk(&[1, 0])?, [9, 10, 0, 0]);
    assert_eq!(array.retrieve_chunk(&[1, 1])?, [0, 0, 0, 0]);

    assert!(array.retrieve_chunk_if_exists(&[0, 0, 0]).is_err());
    assert_eq!(array.retrieve_chunk_if_exists(&[0, 0])?, Some(vec![1, 2, 5, 6]));
    assert_eq!(array.retrieve_chunk_if_exists(&[0, 1])?, Some(vec![3, 4, 7, 8]));
    assert_eq!(array.retrieve_chunk_if_exists(&[1, 0])?, Some(vec![9, 10, 0, 0]));
    assert_eq!(array.retrieve_chunk_if_exists(&[1, 1])?, None);

    assert!(array.retrieve_chunk_ndarray::<u16>(&[0, 0]).is_err());
    assert_eq!(array.retrieve_chunk_ndarray::<u8>(&[0, 0])?, ndarray::array![[1, 2], [5, 6]].into_dyn());
    assert_eq!(array.retrieve_chunk_ndarray::<u8>(&[0, 1])?, ndarray::array![[3, 4], [7, 8]].into_dyn());
    assert_eq!(array.retrieve_chunk_ndarray::<u8>(&[1, 0])?, ndarray::array![[9, 10], [0, 0]].into_dyn());
    assert_eq!(array.retrieve_chunk_ndarray::<u8>(&[1, 1])?, ndarray::array![[0, 0], [0, 0]].into_dyn());

    assert_eq!(array.retrieve_chunk_ndarray_if_exists::<u8>(&[0, 0])?, Some(ndarray::array![[1, 2], [5, 6]].into_dyn()));
    assert_eq!(array.retrieve_chunk_ndarray_if_exists::<u8>(&[0, 1])?, Some(ndarray::array![[3, 4], [7, 8]].into_dyn()));
    assert_eq!(array.retrieve_chunk_ndarray_if_exists::<u8>(&[1, 0])?, Some(ndarray::array![[9, 10], [0, 0]].into_dyn()));
    assert_eq!(array.retrieve_chunk_ndarray_if_exists::<u8>(&[1, 1])?, None);

    assert!(array.retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..2])).is_err());
    assert!(array.retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..3, 0..3])).is_err());
    assert_eq!(array.retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2]))?, [1, 2, 5, 6]);
    assert_eq!(array.retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2]))?, [1, 2]);
    assert_eq!(array.retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 1..2]))?, [2, 6]);

    assert!(array.retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..3, 0..3])).is_err());
    assert!(array.retrieve_chunk_subset_ndarray::<u16>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2])).is_err());
    assert_eq!(array.retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2]))?, ndarray::array![[1, 2], [5, 6]].into_dyn());
    assert_eq!(array.retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2]))?, ndarray::array![[1, 2]].into_dyn());
    assert_eq!(array.retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 1..2]))?, ndarray::array![[2], [6]].into_dyn());

    assert!(array.retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2])).is_err());
    assert_eq!(array.retrieve_chunks(&ArraySubset::new_with_ranges(&[0..0, 0..0]))?, Vec::<u8>::new());
    assert_eq!(array.retrieve_chunks(&ArraySubset::new_with_ranges(&[0..1, 0..1]))?, [1, 2, 5, 6]);
    assert_eq!(array.retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2, 0..2]))?, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0]);
    assert_eq!(array.retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2, 1..2]))?, [3, 4, 7, 8, 0, 0, 0, 0]);
    assert_eq!(array.retrieve_chunks(&ArraySubset::new_with_ranges(&[0..1, 1..3]))?, [3, 4, 0, 0, 7, 8, 0, 0]);

    assert!(array.retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2])).is_err());
    assert!(array.retrieve_chunks_ndarray::<u16>(&ArraySubset::new_with_ranges(&[0..2, 0..2])).is_err());
    assert_eq!(array.retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2, 0..2]))?, ndarray::array![[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 0, 0], [0, 0, 0, 0]].into_dyn());
    assert_eq!(array.retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2, 1..2]))?, ndarray::array![[3, 4], [7, 8], [0, 0], [0, 0]].into_dyn());
    assert_eq!(array.retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..1, 1..3]))?, ndarray::array![[3, 4, 0, 0], [7, 8, 0, 0]].into_dyn());

    assert!(array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..4])).is_err());
    assert_eq!(array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..0, 0..0]))?, Vec::<u8>::new());
    assert_eq!(array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..0, 0..0]))?, [] as [u8; 0]);
    assert_eq!(array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..2, 0..2]))?, [1, 2, 5, 6]);
    assert_eq!(array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..4, 0..4]))?, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0]);
    assert_eq!(array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[1..3, 1..3]))?, [6, 7, 10 ,0]);
    assert_eq!(array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[5..7, 5..6]))?, [0, 0]); // OOB -> fill value
    assert_eq!(array.retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..5, 0..5]))?, [1, 2, 3, 4, 0, 5, 6, 7, 8, 0, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); // OOB -> fill value

    assert!(array.retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..4])).is_err());
    assert!(array.retrieve_array_subset_ndarray::<u16>(&ArraySubset::new_with_ranges(&[0..4, 0..4])).is_err());
    assert_eq!(array.retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..0, 0..0]))?, ndarray::Array2::<u8>::zeros((0, 0)).into_dyn());
    assert_eq!(array.retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..4, 0..4]))?, ndarray::array![[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 0, 0], [0, 0, 0, 0]].into_dyn());
    assert_eq!(array.retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[1..3, 1..3]))?, ndarray::array![[6, 7], [10 ,0]].into_dyn());
    assert_eq!(array.retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[5..7, 5..6]))?, ndarray::array![[0], [0]].into_dyn()); // OOB -> fill value
    assert_eq!(array.retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..5, 0..5]))?, ndarray::array![[1, 2, 3, 4, 0], [5, 6, 7, 8, 0], [9, 10, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]].into_dyn()); // OOB -> fill value

    {
        // Invalid array view dimensionality
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        assert!(array.retrieve_chunk_subset_into_array_view(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2]), &array_view).is_err());
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.retrieve_chunk_subset_into_array_view(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2]), &array_view)?;
        assert_eq!(data, [0, 0, 1, 2, 0, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[0..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.retrieve_chunk_into_array_view(&[0, 0], &array_view)?;
        assert_eq!(data, [1, 2, 5, 6, 0, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.retrieve_array_subset_into_array_view(&ArraySubset::new_with_ranges(&[0..1,0..2]), &array_view)?;
        assert_eq!(data, [0, 0, 1, 2, 0, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..3, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.retrieve_array_subset_into_array_view(&ArraySubset::new_with_ranges(&[0..2,0..2]), &array_view)?;
        assert_eq!(data, [0, 0, 1, 2, 5, 6]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..3, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.retrieve_array_subset_into_array_view(&ArraySubset::new_with_ranges(&[1..3, 1..3]), &array_view)?;
        assert_eq!(data, [0, 0, 6, 7, 10, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0, 0, 0];
        let shape = &[4, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..4, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.retrieve_array_subset_into_array_view(&ArraySubset::new_with_ranges(&[1..4, 0..2]), &array_view)?;
        assert_eq!(data, [0, 0, 5, 6, 9, 10, 0, 0]);
    }

    assert!(array.partial_decoder(&[0]).is_err());
    assert!(array.partial_decoder(&[0, 0])?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1])]).is_err());
    assert_eq!(array.partial_decoder(&[0, 0])?.partial_decode(&[])?, Vec::<Vec<u8>>::new());
    assert_eq!(array.partial_decoder(&[5, 0])?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1, 0..2])])?, [vec![0, 0]]); // OOB -> fill value
    assert_eq!(array.partial_decoder(&[0, 0])?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1, 0..2]), ArraySubset::new_with_ranges(&[0..2, 1..2])])?, [vec![1, 2], vec![2, 6]]);
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        assert!(array.partial_decoder(&[0, 0])?.partial_decode_into_array_view(&ArraySubset::new_with_ranges(&[0..1, 0..2]), &array_view).is_ok());
        assert_eq!(data, [0, 0, 1, 2, 0, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        assert!(array.partial_decoder(&[0, 0])?.partial_decode_into_array_view(&ArraySubset::new_with_ranges(&[0..2, 0..2]), &array_view).is_err());
    }

    Ok(())
}

#[cfg(feature = "ndarray")]
#[test]
fn array_sync_read_uncompressed() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemoryStore::default());
    let array_path = "/array";
    let array = ArrayBuilder::new(
        vec![4, 4], // array shape
        DataType::UInt8,
        vec![2, 2].try_into().unwrap(), // regular chunk shape
        FillValue::from(0u8),
    )
    .bytes_to_bytes_codecs(vec![])
    // .storage_transformers(vec![].into())
    .build(store, array_path)
    .unwrap();
    array_sync_read(array)
}

#[cfg(feature = "ndarray")]
#[test]
fn array_sync_read_shard_compress() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemoryStore::default());
    let array_path = "/array";
    let array = ArrayBuilder::new(
        vec![4, 4], // array shape
        DataType::UInt8,
        vec![2, 2].try_into().unwrap(), // regular chunk shape
        FillValue::from(0u8),
    )
    .array_to_bytes_codec(Box::new(
        ShardingCodecBuilder::new(vec![1, 1].try_into().unwrap())
            .bytes_to_bytes_codecs(vec![
                #[cfg(feature = "gzip")]
                Box::new(GzipCodec::new(5)?),
            ])
            .build(),
    ))
    // .storage_transformers(vec![].into())
    .build(store, array_path)
    .unwrap();
    array_sync_read(array)
}
