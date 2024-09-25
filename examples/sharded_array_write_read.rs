use itertools::Itertools;
use zarrs::{
    array::bytes_to_ndarray,
    storage::{
        storage_adapter::usage_log::UsageLogStorageAdapter, ReadableWritableListableStorage,
    },
};

fn sharded_array_write_read() -> Result<(), Box<dyn std::error::Error>> {
    use zarrs::{
        array::{
            codec::{self, array_to_bytes::sharding::ShardingCodecBuilder},
            DataType, FillValue,
        },
        array_subset::ArraySubset,
        node::Node,
        storage::store,
    };

    use rayon::prelude::{IntoParallelIterator, ParallelIterator};
    use std::sync::Arc;

    // Create a store
    // let path = tempfile::TempDir::new()?;
    // let mut store: ReadableWritableListableStorage =
    //     Arc::new(zarrs::filesystem::FilesystemStore::new(path.path())?);
    // let mut store: ReadableWritableListableStorage = Arc::new(
    //     zarrs::filesystem::FilesystemStore::new("zarrs/tests/data/sharded_array_write_read.zarr")?,
    // );
    let mut store: ReadableWritableListableStorage = Arc::new(store::MemoryStore::new());
    if let Some(arg1) = std::env::args().collect::<Vec<_>>().get(1) {
        if arg1 == "--usage-log" {
            let log_writer = Arc::new(std::sync::Mutex::new(
                // std::io::BufWriter::new(
                std::io::stdout(),
                //    )
            ));
            store = Arc::new(UsageLogStorageAdapter::new(store, log_writer, || {
                chrono::Utc::now().format("[%T%.3f] ").to_string()
            }));
        }
    }

    // Create the root group
    zarrs::group::GroupBuilder::new()
        .build(store.clone(), "/")?
        .store_metadata()?;

    // Create a group with attributes
    let group_path = "/group";
    let mut group = zarrs::group::GroupBuilder::new().build(store.clone(), group_path)?;
    group
        .attributes_mut()
        .insert("foo".into(), serde_json::Value::String("bar".into()));
    group.store_metadata()?;

    // Create an array
    let array_path = "/group/array";
    let shard_shape = vec![4, 8];
    let inner_chunk_shape = vec![4, 4];
    let mut sharding_codec_builder =
        ShardingCodecBuilder::new(inner_chunk_shape.as_slice().try_into()?);
    sharding_codec_builder.bytes_to_bytes_codecs(vec![
        #[cfg(feature = "gzip")]
        Arc::new(codec::GzipCodec::new(5)?),
    ]);
    let array = zarrs::array::ArrayBuilder::new(
        vec![8, 8], // array shape
        DataType::UInt16,
        shard_shape.try_into()?,
        FillValue::from(0u16),
    )
    .array_to_bytes_codec(Arc::new(sharding_codec_builder.build()))
    .dimension_names(["y", "x"].into())
    // .storage_transformers(vec![].into())
    .build(store.clone(), array_path)?;

    // Write array metadata to store
    array.store_metadata()?;

    // The array metadata is
    println!(
        "The array metadata is:\n{}\n",
        serde_json::to_string_pretty(&array.metadata()).unwrap()
    );

    // Write some shards (in parallel)
    (0..2).into_par_iter().try_for_each(|s| {
        let chunk_grid = array.chunk_grid();
        let chunk_indices = vec![s, 0];
        if let Some(chunk_shape) = chunk_grid.chunk_shape(&chunk_indices, array.shape())? {
            let chunk_array = ndarray::ArrayD::<u16>::from_shape_fn(
                chunk_shape
                    .iter()
                    .map(|u| u.get() as usize)
                    .collect::<Vec<_>>(),
                |ij| {
                    (s * chunk_shape[0].get() * chunk_shape[1].get()
                        + ij[0] as u64 * chunk_shape[1].get()
                        + ij[1] as u64) as u16
                },
            );
            array.store_chunk_ndarray(&chunk_indices, chunk_array)
        } else {
            Err(zarrs::array::ArrayError::InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
            ))
        }
    })?;

    // Read the whole array
    let data_all = array.retrieve_array_subset_ndarray::<u16>(&array.subset_all())?;
    println!("The whole array is:\n{data_all}\n");

    // Read a shard back from the store
    let shard_indices = vec![1, 0];
    let data_shard = array.retrieve_chunk_ndarray::<u16>(&shard_indices)?;
    println!("Shard [1,0] is:\n{data_shard}\n");

    // Read an inner chunk from the store
    let subset_chunk_1_0 = ArraySubset::new_with_ranges(&[4..8, 0..4]);
    let data_chunk = array.retrieve_array_subset_ndarray::<u16>(&subset_chunk_1_0)?;
    println!("Chunk [1,0] is:\n{data_chunk}\n");

    // Read the central 4x2 subset of the array
    let subset_4x2 = ArraySubset::new_with_ranges(&[2..6, 3..5]); // the center 4x2 region
    let data_4x2 = array.retrieve_array_subset_ndarray::<u16>(&subset_4x2)?;
    println!("The middle 4x2 subset is:\n{data_4x2}\n");

    // Decode inner chunks
    // In some cases, it might be preferable to decode inner chunks in a shard directly.
    // If using the partial decoder, then the shard index will only be read once from the store.
    let partial_decoder = array.partial_decoder(&[0, 0])?;
    let inner_chunks_to_decode = vec![
        ArraySubset::new_with_start_shape(vec![0, 0], inner_chunk_shape.clone())?,
        ArraySubset::new_with_start_shape(vec![0, 4], inner_chunk_shape.clone())?,
    ];
    let decoded_inner_chunks_bytes = partial_decoder.partial_decode(&inner_chunks_to_decode)?;
    println!("Decoded inner chunks:");
    for (inner_chunk_subset, decoded_inner_chunk) in
        std::iter::zip(inner_chunks_to_decode, decoded_inner_chunks_bytes)
    {
        let ndarray = bytes_to_ndarray::<u16>(
            &inner_chunk_shape,
            decoded_inner_chunk.into_fixed()?.into_owned(),
        )?;
        println!("{inner_chunk_subset}\n{ndarray}\n");
    }

    // Show the hierarchy
    let node = Node::open(&store, "/").unwrap();
    let tree = node.hierarchy_tree();
    println!("The Zarr hierarchy tree is:\n{}", tree);

    println!(
        "The keys in the store are:\n[{}]",
        store.list().unwrap_or_default().iter().format(", ")
    );

    Ok(())
}

fn main() {
    if let Err(err) = sharded_array_write_read() {
        println!("{:?}", err);
    }
}
