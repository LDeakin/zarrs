#![allow(missing_docs)]
#![cfg(feature = "sharding")]

use std::sync::Arc;

use zarrs::{
    array::{
        codec::{
            array_to_bytes::sharding::ShardingCodecBuilder, BytesToBytesCodecTraits,
            CodecOptionsBuilder,
        },
        ArrayBuilder, DataType, FillValue,
    },
    array_subset::ArraySubset,
};
use zarrs_metadata_ext::codec::sharding::ShardingIndexLocation;
use zarrs_storage::{
    storage_adapter::performance_metrics::PerformanceMetricsStorageAdapter, store::MemoryStore,
    ReadableStorageTraits,
};

fn array_partial_encode_sharding(
    sharding_index_location: ShardingIndexLocation,
    inner_bytes_to_bytes_codecs: Vec<Arc<dyn BytesToBytesCodecTraits>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let opt = CodecOptionsBuilder::new()
        .experimental_partial_encoding(true)
        .build();

    let store = std::sync::Arc::new(MemoryStore::default());
    // let log_writer = Arc::new(std::sync::Mutex::new(
    //     // std::io::BufWriter::new(
    //     std::io::stdout(),
    //     //    )
    // ));
    // let store = Arc::new(
    //     zarrs_storage::storage_adapter::usage_log::UsageLogStorageAdapter::new(
    //         store.clone(),
    //         log_writer.clone(),
    //         || chrono::Utc::now().format("[%T%.3f] ").to_string(),
    //     ),
    // );
    let store_perf = Arc::new(PerformanceMetricsStorageAdapter::new(store.clone()));

    let array_path = "/";
    let mut builder = ArrayBuilder::new(
        vec![4, 4], // array shape
        DataType::UInt16,
        vec![2, 2].try_into().unwrap(), // regular chunk shape
        FillValue::from(0u16),
    );
    builder
        .array_to_bytes_codec(Arc::new(
            ShardingCodecBuilder::new(vec![1, 1].try_into().unwrap())
                .index_bytes_to_bytes_codecs(vec![])
                .index_location(sharding_index_location)
                .bytes_to_bytes_codecs(inner_bytes_to_bytes_codecs.clone())
                .build(),
        ))
        .bytes_to_bytes_codecs(vec![]);
    // .storage_transformers(vec![].into())

    let array = builder.build(store_perf.clone(), array_path).unwrap();

    let get_bytes_0_0 = || {
        let key = array.chunk_key_encoding().encode(&[0, 0]);
        store.get(&key)
    };

    let expected_writes_per_shard = match sharding_index_location {
        ShardingIndexLocation::Start => 2, // Separate write for inner chunks and index
        ShardingIndexLocation::End => 1,   // Combined write for inner chunks and index
    };

    let chunks_per_shard = 2 * 2;
    let shard_index_size = size_of::<u64>() * 2 * chunks_per_shard;
    assert!(get_bytes_0_0()?.is_none());
    assert_eq!(store_perf.reads(), 0);
    assert_eq!(store_perf.bytes_read(), 0);

    // [1, 0]
    // [0, 0]
    array.store_array_subset_elements_opt::<u16>(
        &ArraySubset::new_with_ranges(&[0..1, 0..1]),
        &[1],
        &opt,
    )?;
    assert_eq!(store_perf.reads(), 1); // index
    assert_eq!(store_perf.writes(), expected_writes_per_shard);
    assert_eq!(store_perf.bytes_read(), 0);
    if inner_bytes_to_bytes_codecs.is_empty() {
        assert_eq!(
            get_bytes_0_0()?.unwrap().len(),
            shard_index_size + size_of::<u16>() * 1
        );
    }
    store_perf.reset();

    // [0, 0]
    // [0, 0]
    array.store_array_subset_elements_opt::<u16>(
        &ArraySubset::new_with_ranges(&[0..1, 0..1]),
        &[0],
        &opt,
    )?;
    assert_eq!(store_perf.reads(), 1); // index
    assert_eq!(store_perf.writes(), 0);
    if inner_bytes_to_bytes_codecs.is_empty() {
        assert_eq!(store_perf.bytes_read(), shard_index_size * 1);
    }
    assert!(get_bytes_0_0()?.is_none());
    store_perf.reset();

    // [1, 2]
    // [0, 0]
    array.store_array_subset_elements_opt::<u16>(
        &ArraySubset::new_with_ranges(&[0..1, 0..2]),
        &[1, 2],
        &opt,
    )?;
    assert_eq!(store_perf.reads(), 1); // index
    assert_eq!(store_perf.writes(), expected_writes_per_shard);
    if inner_bytes_to_bytes_codecs.is_empty() {
        assert_eq!(
            get_bytes_0_0()?.unwrap().len(),
            shard_index_size + size_of::<u16>() * 2
        );
    }
    assert_eq!(
        array.retrieve_chunk_elements::<u16>(&[0, 0])?,
        vec![1, 2, 0, 0]
    );
    store_perf.reset();

    // Check that the shard is entirely rewritten when possible, rather than appended
    // [3, 4]
    // [0, 0]
    array.store_array_subset_elements_opt::<u16>(
        &ArraySubset::new_with_ranges(&[0..1, 0..2]),
        &[3, 4],
        &opt,
    )?;
    assert_eq!(store_perf.reads(), 1); // index + 1x inner chunk
    assert_eq!(store_perf.writes(), expected_writes_per_shard);
    if inner_bytes_to_bytes_codecs.is_empty() {
        assert_eq!(store_perf.bytes_read(), shard_index_size * 1);
    }
    if inner_bytes_to_bytes_codecs.is_empty() {
        assert_eq!(
            get_bytes_0_0()?.unwrap().len(),
            shard_index_size + size_of::<u16>() * 2
        );
    }
    assert_eq!(
        array.retrieve_chunk_elements::<u16>(&[0, 0])?,
        vec![3, 4, 0, 0]
    );
    store_perf.reset();

    // [99, 4]
    // [5, 0]
    array.store_array_subset_elements_opt::<u16>(
        &ArraySubset::new_with_ranges(&[0..2, 0..1]),
        &[99, 5],
        &opt,
    )?;
    assert_eq!(store_perf.reads(), 1); // index
    assert_eq!(store_perf.writes(), expected_writes_per_shard);
    if inner_bytes_to_bytes_codecs.is_empty() {
        assert_eq!(
            get_bytes_0_0()?.unwrap().len(),
            shard_index_size + size_of::<u16>() * 4 // 1 stale inner chunk + 3 inner chunks
        );
    }
    assert_eq!(
        array.retrieve_chunk_elements::<u16>(&[0, 0])?,
        vec![99, 4, 5, 0]
    );
    store_perf.reset();

    // [99, 4]
    // [5, 100]
    store_perf.reset();
    array.store_array_subset_elements_opt::<u16>(
        &ArraySubset::new_with_ranges(&[1..2, 1..2]),
        &[100],
        &opt,
    )?;
    assert_eq!(store_perf.reads(), 1); // index
    assert_eq!(store_perf.writes(), expected_writes_per_shard);
    if inner_bytes_to_bytes_codecs.is_empty() {
        assert_eq!(
            get_bytes_0_0()?.unwrap().len(),
            shard_index_size + size_of::<u16>() * 5 // 1 stale inner chunk + 4 inner chunks
        );
    }
    store_perf.reset();

    assert_eq!(
        array.retrieve_chunk_elements::<u16>(&[0, 0])?,
        vec![99, 4, 5, 100]
    );

    Ok(())
}

#[test]
fn array_partial_encode_sharding_index_start() {
    array_partial_encode_sharding(ShardingIndexLocation::Start, vec![]).unwrap();
}

#[test]
fn array_partial_encode_sharding_index_end() {
    array_partial_encode_sharding(ShardingIndexLocation::End, vec![]).unwrap();
}

#[cfg(all(
    feature = "gzip",
    feature = "bz2",
    feature = "blosc",
    feature = "crc32c",
    feature = "zstd"
))]
#[test]
fn array_partial_encode_sharding_index_compressed() {
    use zarrs_metadata_ext::codec::blosc::{
        BloscCompressionLevel, BloscCompressor, BloscShuffleMode,
    };
    use zarrs_metadata_ext::codec::bz2::Bz2CompressionLevel;

    for index_location in &[ShardingIndexLocation::Start, ShardingIndexLocation::End] {
        array_partial_encode_sharding(
            *index_location,
            vec![
                Arc::new(zarrs::array::codec::GzipCodec::new(5).unwrap()),
                Arc::new(zarrs::array::codec::ZstdCodec::new(
                    5.try_into().unwrap(),
                    true,
                )),
                Arc::new(zarrs::array::codec::Bz2Codec::new(
                    Bz2CompressionLevel::try_from(5u8).unwrap(),
                )),
                Arc::new(
                    zarrs::array::codec::BloscCodec::new(
                        BloscCompressor::BloscLZ,
                        BloscCompressionLevel::try_from(5u8).unwrap(),
                        None,
                        BloscShuffleMode::NoShuffle,
                        None,
                    )
                    .unwrap(),
                ),
                Arc::new(zarrs::array::codec::Crc32cCodec::new()),
            ],
        )
        .unwrap();
    }
}
