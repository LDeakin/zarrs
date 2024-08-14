#![cfg(all(feature = "async", feature = "ndarray", feature = "object_store"))]

use zarrs::array::codec::array_to_bytes::vlen::VlenCodec;
use zarrs::array::codec::TransposeCodec;
use zarrs::array::{Array, ArrayBuilder, DataType, FillValue};
use zarrs::array_subset::ArraySubset;

use object_store::memory::InMemory;

use zarrs::metadata::v3::codec::transpose::TransposeOrder;
use zarrs::storage::store::AsyncObjectStore;

#[rustfmt::skip]
async fn array_async_read(shard: bool) -> Result<(), Box<dyn std::error::Error>> {
    let store = std::sync::Arc::new(AsyncObjectStore::new(InMemory::new()));
    let array_path = "/array";
    let mut builder = ArrayBuilder::new(
        vec![4, 4], // array shape
        DataType::UInt8,
        vec![2, 2].try_into().unwrap(), // regular chunk shape
        FillValue::from(0u8),
    );
    builder.bytes_to_bytes_codecs(vec![]);
    // builder.storage_transformers(vec![].into());
    if shard {
        #[cfg(feature = "sharding")]
        builder.array_to_bytes_codec(Box::new(
            zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder::new(vec![1, 1].try_into().unwrap())
                .bytes_to_bytes_codecs(vec![
                    #[cfg(feature = "gzip")]
                    Box::new(zarrs::array::codec::GzipCodec::new(5)?),
                ])
                .build(),
        ));
    }
    let array = builder.build(store, array_path).unwrap();

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
    array.async_store_chunk(&[0, 0], &[1, 2, 0, 0]).await?;
    array.async_store_chunk(&[0, 1], &[3, 4, 7, 8]).await?;
    array.async_store_array_subset(&ArraySubset::new_with_ranges(&[1..3, 0..2]), &[5, 6, 9, 10]).await?;

    assert!(array.async_retrieve_chunk(&[0, 0, 0]).await.is_err());
    assert_eq!(array.async_retrieve_chunk(&[0, 0]).await?, vec![1, 2, 5, 6].into());
    assert_eq!(array.async_retrieve_chunk(&[0, 1]).await?, vec![3, 4, 7, 8].into());
    assert_eq!(array.async_retrieve_chunk(&[1, 0]).await?, vec![9, 10, 0, 0].into());
    assert_eq!(array.async_retrieve_chunk(&[1, 1]).await?, vec![0, 0, 0, 0].into());

    assert!(array.async_retrieve_chunk_if_exists(&[0, 0, 0]).await.is_err());
    assert_eq!(array.async_retrieve_chunk_if_exists(&[0, 0]).await?, Some(vec![1, 2, 5, 6].into()));
    assert_eq!(array.async_retrieve_chunk_if_exists(&[0, 1]).await?, Some(vec![3, 4, 7, 8].into()));
    assert_eq!(array.async_retrieve_chunk_if_exists(&[1, 0]).await?, Some(vec![9, 10, 0, 0].into()));
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
    assert_eq!(array.async_retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, vec![1, 2, 5, 6].into());
    assert_eq!(array.async_retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2])).await?, vec![1, 2].into());
    assert_eq!(array.async_retrieve_chunk_subset(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 1..2])).await?, vec![2, 6].into());

    assert!(array.async_retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..3, 0..3])).await.is_err());
    assert!(array.async_retrieve_chunk_subset_ndarray::<u16>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2])).await.is_err());
    assert_eq!(array.async_retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, ndarray::array![[1, 2], [5, 6]].into_dyn());
    assert_eq!(array.async_retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..1, 0..2])).await?, ndarray::array![[1, 2]].into_dyn());
    assert_eq!(array.async_retrieve_chunk_subset_ndarray::<u8>(&[0, 0], &ArraySubset::new_with_ranges(&[0..2, 1..2])).await?, ndarray::array![[2], [6]].into_dyn());

    assert!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2])).await.is_err());
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..0, 0..0])).await?, vec![].into());
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..1, 0..1])).await?, vec![1, 2, 5, 6].into());
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0].into());
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..2, 1..2])).await?, vec![3, 4, 7, 8, 0, 0, 0, 0].into());
    assert_eq!(array.async_retrieve_chunks(&ArraySubset::new_with_ranges(&[0..1, 1..3])).await?, vec![3, 4, 0, 0, 7, 8, 0, 0].into());

    assert!(array.async_retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2])).await.is_err());
    assert!(array.async_retrieve_chunks_ndarray::<u16>(&ArraySubset::new_with_ranges(&[0..2, 0..2])).await.is_err());
    assert_eq!(array.async_retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, ndarray::array![[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 0, 0], [0, 0, 0, 0]].into_dyn());
    assert_eq!(array.async_retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..2, 1..2])).await?, ndarray::array![[3, 4], [7, 8], [0, 0], [0, 0]].into_dyn());
    assert_eq!(array.async_retrieve_chunks_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..1, 1..3])).await?, ndarray::array![[3, 4, 0, 0], [7, 8, 0, 0]].into_dyn());

    assert!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..4])).await.is_err());
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..0, 0..0])).await?, vec![].into());
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..2, 0..2])).await?, vec![1, 2, 5, 6].into());
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..4, 0..4])).await?, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0].into());
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[1..3, 1..3])).await?, vec![6, 7, 10 ,0].into());
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[5..7, 5..6])).await?, vec![0, 0].into()); // OOB -> fill value
    assert_eq!(array.async_retrieve_array_subset(&ArraySubset::new_with_ranges(&[0..5, 0..5])).await?, vec![1, 2, 3, 4, 0, 5, 6, 7, 8, 0, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into()); // OOB -> fill value

    assert!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..4])).await.is_err());
    assert!(array.async_retrieve_array_subset_ndarray::<u16>(&ArraySubset::new_with_ranges(&[0..4, 0..4])).await.is_err());
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..0, 0..0])).await?, ndarray::Array2::<u8>::zeros((0, 0)).into_dyn());
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..4, 0..4])).await?, ndarray::array![[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 0, 0], [0, 0, 0, 0]].into_dyn());
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[1..3, 1..3])).await?, ndarray::array![[6, 7], [10 ,0]].into_dyn());
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[5..7, 5..6])).await?, ndarray::array![[0], [0]].into_dyn()); // OOB -> fill value
    assert_eq!(array.async_retrieve_array_subset_ndarray::<u8>(&ArraySubset::new_with_ranges(&[0..5, 0..5])).await?, ndarray::array![[1, 2, 3, 4, 0], [5, 6, 7, 8, 0], [9, 10, 0, 0, 0], [0, 0, 0, 0, 0], [0, 0, 0, 0, 0]].into_dyn()); // OOB -> fill value

    assert!(array.async_partial_decoder(&[0]).await.is_err());
    assert!(array.async_partial_decoder(&[0, 0]).await?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1])]).await.is_err());
    assert_eq!(array.async_partial_decoder(&[0, 0]).await?.partial_decode(&[]).await?, []);
    assert_eq!(array.async_partial_decoder(&[5, 0]).await?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1, 0..2])]).await?, [vec![0, 0].into()]); // OOB -> fill value
    assert_eq!(array.async_partial_decoder(&[0, 0]).await?.partial_decode(&[ArraySubset::new_with_ranges(&[0..1, 0..2]), ArraySubset::new_with_ranges(&[0..2, 1..2])]).await?, [vec![1, 2].into(), vec![2, 6].into()]);

    Ok(())
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // FIXME: Check if this failure is real
async fn array_async_read_uncompressed() -> Result<(), Box<dyn std::error::Error>> {
    array_async_read(false).await
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn array_async_read_shard_compress() -> Result<(), Box<dyn std::error::Error>> {
    array_async_read(true).await
}

async fn array_str_impl(
    array: Array<AsyncObjectStore<InMemory>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Store a single chunk
    array
        .async_store_chunk_elements(&[0, 0], &["a", "bb", "ccc", "dddd"])
        .await?;
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[0, 0])
            .await?,
        &["a", "bb", "ccc", "dddd"]
    );

    // Write array subset with full chunks
    array
        .async_store_array_subset_elements(
            &ArraySubset::new_with_ranges(&[2..4, 0..4]),
            &[
                "1", "22", "333", "4444", "55555", "666666", "7777777", "88888888",
            ],
        )
        .await?;
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[1, 0])
            .await?,
        &["1", "22", "55555", "666666"]
    );
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[1, 1])
            .await?,
        &["333", "4444", "7777777", "88888888"]
    );

    // Write array subset with partial chunks
    array
        .async_store_array_subset_elements(
            &ArraySubset::new_with_ranges(&[1..3, 1..3]),
            &["S1", "S22", "S333", "S4444"],
        )
        .await?;
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[0, 0])
            .await?,
        &["a", "bb", "ccc", "S1"]
    );
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[0, 1])
            .await?,
        &["", "", "S22", ""]
    );
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[1, 0])
            .await?,
        &["1", "S333", "55555", "666666"]
    );
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[1, 1])
            .await?,
        &["S4444", "4444", "7777777", "88888888"]
    );

    // Write multiple chunks
    array
        .async_store_chunks_elements(
            &ArraySubset::new_with_ranges(&[0..1, 0..2]),
            &["a", "bb", "ccc", "dddd", "C0", "C11", "C222", "C3333"],
        )
        .await?;
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[0, 0])
            .await?,
        &["a", "bb", "C0", "C11"]
    );
    assert_eq!(
        array
            .async_retrieve_chunk_elements::<String>(&[0, 1])
            .await?,
        &["ccc", "dddd", "C222", "C3333"]
    );
    assert_eq!(
        array
            .async_retrieve_chunks_elements::<String>(&ArraySubset::new_with_ranges(&[0..1, 0..2]))
            .await?,
        &["a", "bb", "ccc", "dddd", "C0", "C11", "C222", "C3333"]
    );

    // Full chunk requests
    assert_eq!(
        array
            .async_retrieve_array_subset_elements::<String>(&ArraySubset::new_with_ranges(&[
                0..4,
                0..4
            ]))
            .await?,
        &[
            "a", "bb", "ccc", "dddd", "C0", "C11", "C222", "C3333", //
            "1", "S333", "S4444", "4444", "55555", "666666", "7777777", "88888888" //
        ]
    );

    // Partial chunk requests
    assert_eq!(
        array
            .async_retrieve_array_subset_elements::<String>(&ArraySubset::new_with_ranges(&[
                1..3,
                1..3
            ]))
            .await?,
        &["C11", "C222", "S333", "S4444"]
    );

    // Incompatible chunks / bytes
    assert!(array
        .async_store_chunks_elements(&ArraySubset::new_with_ranges(&[0..0, 0..2]), &["a", "bb"])
        .await
        .is_err());
    assert!(array
        .async_store_chunks_elements(&ArraySubset::new_with_ranges(&[0..1, 0..2]), &["a", "bb"])
        .await
        .is_err());

    Ok(())
}

#[tokio::test]
async fn array_str_async_simple() -> Result<(), Box<dyn std::error::Error>> {
    let store = std::sync::Arc::new(AsyncObjectStore::new(InMemory::new()));
    let array_path = "/array";
    let mut builder = ArrayBuilder::new(
        vec![4, 4], // array shape
        DataType::String,
        vec![2, 2].try_into().unwrap(), // regular chunk shape
        FillValue::from(""),
    );
    builder.bytes_to_bytes_codecs(vec![
        #[cfg(feature = "gzip")]
        Box::new(zarrs::array::codec::GzipCodec::new(5)?),
    ]);

    let array = builder.build(store, array_path).unwrap();
    array_str_impl(array).await
}

#[tokio::test]
async fn array_str_async_sharded_transpose() -> Result<(), Box<dyn std::error::Error>> {
    let store = std::sync::Arc::new(AsyncObjectStore::new(InMemory::new()));
    let array_path = "/array";
    let mut builder = ArrayBuilder::new(
        vec![4, 4], // array shape
        DataType::String,
        vec![2, 2].try_into().unwrap(), // regular chunk shape
        FillValue::from(""),
    );
    builder.array_to_array_codecs(vec![Box::new(TransposeCodec::new(
        TransposeOrder::new(&[1, 0]).unwrap(),
    ))]);
    builder.array_to_bytes_codec(Box::new(
        zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder::new(
            vec![2, 1].try_into().unwrap(),
        )
        .array_to_bytes_codec(Box::<VlenCodec>::default())
        .bytes_to_bytes_codecs(vec![
            #[cfg(feature = "gzip")]
            Box::new(zarrs::array::codec::GzipCodec::new(5)?),
        ])
        .build(),
    ));

    let array = builder.build(store, array_path).unwrap();
    array_str_impl(array).await
}
