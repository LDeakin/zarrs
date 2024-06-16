use std::sync::Arc;

use zarrs::array::codec::{array_to_bytes::sharding::ShardingCodecBuilder, GzipCodec};
use zarrs::array::{Array, ArrayBuilder, ArrayView, DataType, FillValue};
use zarrs::array_subset::ArraySubset;

#[cfg(feature = "object_store")]
use object_store::memory::InMemory;

#[cfg(all(feature = "async", feature = "object_store"))]
use zarrs::storage::store::AsyncObjectStore;

#[cfg(all(feature = "ndarray", feature = "async", feature = "object_store"))]
#[rustfmt::skip]
async fn array_async_read(array: Arc<Array<AsyncObjectStore<InMemory>>>) -> Result<(), Box<dyn std::error::Error>> {
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
    array.async_store_chunk(&[0, 0], vec![1, 2, 0, 0]).await?;
    array.async_store_chunk(&[0, 1], vec![3, 4, 7, 8]).await?;
    array.async_store_array_subset(&ArraySubset::new_with_ranges(&[1..3, 0..2]), vec![5, 6, 9, 10]).await?;

    assert!(array.async_retrieve_chunk(&[0, 0, 0]).await.is_err());
    assert_eq!(array.async_retrieve_chunk(&[0, 0]).await?, [1, 2, 5, 6]);
    assert_eq!(array.async_retrieve_chunk(&[0, 1]).await?, [3, 4, 7, 8]);
    assert_eq!(array.async_retrieve_chunk(&[1, 0]).await?, [9, 10, 0, 0]);
    assert_eq!(array.async_retrieve_chunk(&[1, 1]).await?, [0, 0, 0, 0]);

    assert!(array.async_retrieve_chunk_if_exists(&[0, 0, 0]).await.is_err());
    assert_eq!(array.async_retrieve_chunk_if_exists(&[0, 0]).await?, Some(vec![1, 2, 5, 6]));
    assert_eq!(array.async_retrieve_chunk_if_exists(&[0, 1]).await?, Some(vec![3, 4, 7, 8]));
    assert_eq!(array.async_retrieve_chunk_if_exists(&[1, 0]).await?, Some(vec![9, 10, 0, 0]));
    assert_eq!(array.async_retrieve_chunk_if_exists(&[1, 1]).await?, None);

    assert!(array.async_retrieve_chunk_ndarray::<u16>(&[0, 0]).await.is_err());
    assert_eq!(array.async_retrieve_chunk_ndarray::<u8>(&[0, 0]).await?, ndarray::array![[1, 2], [5, 6]].into_dyn());
    assert_eq!(array.async_retrieve_chunk_ndarray::<u8>(&[0, 1]).await?, ndarray::array![[3, 4], [7, 8]].into_dyn());
    assert_eq!(array.async_retrieve_chunk_ndarray::<u8>(&[1, 0]).await?, ndarray::array![[9, 10], [0, 0]].into_dyn());
    assert_eq!(array.async_retrieve_chunk_ndarray::<u8>(&[1, 1]).await?, ndarray::array![[0, 0], [0, 0]].into_dyn());

    assert_eq!(array.async_retrieve_chunk_ndarray_if_exists::<u8>(&[0, 0]).await?, Some(ndarray::array![[1, 2], [5, 6]].into_dyn()));
    assert_eq!(array.async_retrieve_chunk_ndarray_if_exists::<u8>(&[0, 1]).await?, Some(ndarray::array![[3, 4], [7, 8]].into_dyn()));
    assert_eq!(array.async_retrieve_chunk_ndarray_if_exists::<u8>(&[1, 0]).await?, Some(ndarray::array![[9, 10], [0, 0]].into_dyn()));
    assert_eq!(array.async_retrieve_chunk_ndarray_if_exists::<u8>(&[1, 1]).await?, None);

    assert!(array.async_retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..2])).await.is_err());
    assert!(array.async_retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..3, 0..3])).await.is_err());
    assert_eq!(array.async_retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, [1, 2, 5, 6]);
    assert_eq!(array.async_retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2])).await?, [1, 2]);
    assert_eq!(array.async_retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 1..2])).await?, [2, 6]);

    assert!(array.async_retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..3, 0..3])).await.is_err());
    assert!(array.async_retrieve_chunk_subset_ndarray::<u16>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2])).await.is_err());
    assert_eq!(array.async_retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, ndarray::array![[1, 2], [5, 6]].into_dyn());
    assert_eq!(array.async_retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2])).await?, ndarray::array![[1, 2]].into_dyn());
    assert_eq!(array.async_retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 1..2])).await?, ndarray::array![[2], [6]].into_dyn());

    assert!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2])).await.is_err());
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..0, 0..0])).await?, Vec::<u8>::new());
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..1, 0..1])).await?, [1, 2, 5, 6]);
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0]);
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2, 1..2])).await?, [3, 4, 7, 8, 0, 0, 0, 0]);
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..1, 1..3])).await?, [3, 4, 0, 0, 7, 8, 0, 0]);

    assert!(array.async_retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2])).await.is_err());
    assert!(array.async_retrieve_chunks_ndarray::<u16>(&ArraySubset::new_with_ranges(&[0..2, 0..2])).await.is_err());
    assert_eq!(array.async_retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, ndarray::array![[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 0, 0], [0, 0, 0, 0]].into_dyn());
    assert_eq!(array.async_retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2, 1..2])).await?, ndarray::array![[3, 4], [7, 8], [0, 0], [0, 0]].into_dyn());
    assert_eq!(array.async_retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..1, 1..3])).await?, ndarray::array![[3, 4, 0, 0], [7, 8, 0, 0]].into_dyn());

    assert!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..4])).await.is_err());
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..0, 0..0])).await?, Vec::<u8>::new());
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..0, 0..0])).await?, [] as [u8; 0]);
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, [1, 2, 5, 6]);
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..4, 0..4])).await?, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0]);
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[1..3, 1..3])).await?, [6, 7, 10 ,0]);
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[5..7, 5..6])).await?, [0, 0]); // OOB -> fill value
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..5, 0..5])).await?, [1, 2, 3, 4, 0, 5, 6, 7, 8, 0, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); // OOB -> fill value

    assert!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..4])).await.is_err());
    assert!(array.async_retrieve_array_subset_ndarray::<u16>(&ArraySubset::new_with_ranges(&[0..4, 0..4])).await.is_err());
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..0, 0..0])).await?, ndarray::Array2::<u8>::zeros((0, 0)).into_dyn());
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..4, 0..4])).await?, ndarray::array![[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 0, 0], [0, 0, 0, 0]].into_dyn());
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[1..3, 1..3])).await?, ndarray::array![[6, 7], [10 ,0]].into_dyn());
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[5..7, 5..6])).await?, ndarray::array![[0], [0]].into_dyn()); // OOB -> fill value
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..5, 0..5])).await?, ndarray::array![[1, 2, 3, 4, 0], [5, 6, 7, 8, 0], [9, 10, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]].into_dyn()); // OOB -> fill value

    {
        // Invalid array view dimensionality
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        assert!(array.async_retrieve_chunk_subset_into_array_view(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2]), &array_view).await.is_err());
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_chunk_subset_into_array_view(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2]), &array_view).await?;
        assert_eq!(data, [0, 0, 1, 2, 0, 0]);
    }

    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[0..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_chunk_into_array_view(&[0, 0], &array_view).await?;
        assert_eq!(data, [1, 2, 5, 6, 0, 0]);
    }

    {
        let mut data = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let shape = &[4, 4];
        let array_view_subset = ArraySubset::new_with_ranges(&[0..4, 0..4]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_chunks_into_array_view(&ArraySubset::new_with_ranges(&[0..2, 0..2]), &array_view).await?;
        assert_eq!(data, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let shape = &[3, 4];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..3, 0..4]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_chunks_into_array_view(&ArraySubset::new_with_ranges(&[0..1, 0..2]), &array_view).await?;
        assert_eq!(data, [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]);
    }
    {
        // Test OOB
        let mut data = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let shape = &[3, 4];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..3, 0..4]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_chunks_into_array_view(&ArraySubset::new_with_ranges(&[0..1, 1..3]), &array_view).await?;
        assert_eq!(data, [0, 0, 0, 0, 3, 4, 0, 0, 7, 8, 0, 0]);
    }

    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_array_subset_into_array_view(&ArraySubset::new_with_ranges(&[0..1,0..2]), &array_view).await?;
        assert_eq!(data, [0, 0, 1, 2, 0, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..3, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_array_subset_into_array_view(&ArraySubset::new_with_ranges(&[0..2,0..2]), &array_view).await?;
        assert_eq!(data, [0, 0, 1, 2, 5, 6]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..3, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_array_subset_into_array_view(&ArraySubset::new_with_ranges(&[1..3, 1..3]), &array_view).await?;
        assert_eq!(data, [0, 0, 6, 7, 10, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0, 0, 0];
        let shape = &[4, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..4, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        array.async_retrieve_array_subset_into_array_view(&ArraySubset::new_with_ranges(&[1..4, 0..2]), &array_view).await?;
        assert_eq!(data, [0, 0, 5, 6, 9, 10, 0, 0]);
    }

    assert!(array.async_partial_decoder(&[0]).await.is_err());
    assert!(array.async_partial_decoder(&[0, 0]).await?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1])]).await.is_err());
    assert_eq!(array.async_partial_decoder(&[0, 0]).await?.partial_decode(&[]).await?, Vec::<Vec<u8>>::new());
    assert_eq!(array.async_partial_decoder(&[5, 0]).await?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1, 0..2])]).await?, [vec![0, 0]]); // OOB -> fill value
    assert_eq!(array.async_partial_decoder(&[0, 0]).await?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1, 0..2]), ArraySubset::new_with_ranges(&[0..2, 1..2])]).await?, [vec![1, 2], vec![2, 6]]);
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        assert!(array.async_partial_decoder(&[0, 0]).await?.partial_decode_into_array_view(&ArraySubset::new_with_ranges(&[0..1, 0..2]), &array_view).await.is_ok());
        assert_eq!(data, [0, 0, 1, 2, 0, 0]);
    }
    {
        let mut data = vec![0, 0, 0, 0, 0, 0];
        let shape = &[3, 2];
        let array_view_subset = ArraySubset::new_with_ranges(&[1..2, 0..2]);
        let array_view = ArrayView::new(&mut data, shape, array_view_subset)?;
        assert!(array.async_partial_decoder(&[0, 0]).await?.partial_decode_into_array_view(&ArraySubset::new_with_ranges(&[0..2, 0..2]), &array_view).await.is_err());
    }

    Ok(())
}

#[cfg(all(feature = "ndarray", feature = "async", feature = "object_store"))]
#[tokio::test]
#[cfg_attr(miri, ignore)] // FIXME: Check if this failure is real
async fn array_async_read_uncompressed() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(AsyncObjectStore::new(InMemory::new()));
    let array_path = "/array";
    let array = ArrayBuilder::new(
        vec![4, 4], // array shape
        DataType::UInt8,
        vec![2, 2].try_into().unwrap(), // regular chunk shape
        FillValue::from(0u8),
    )
    .bytes_to_bytes_codecs(vec![])
    // .storage_transformers(vec![].into())
    .build_arc(store, array_path)
    .unwrap();
    array_async_read(array).await
}

#[cfg(all(feature = "ndarray", feature = "async", feature = "object_store"))]
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn array_async_read_shard_compress() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(AsyncObjectStore::new(InMemory::new()));
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
    .build_arc(store, array_path)
    .unwrap();
    array_async_read(array).await
}
