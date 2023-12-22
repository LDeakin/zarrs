fn sharded_array_write_read() -> Result<(), Box<dyn std::error::Error>> {
    use zarrs::{
        array::{
            codec::{self, array_to_bytes::sharding::ShardingCodecBuilder},
            DataType, FillValue,
        },
        array_subset::ArraySubset,
        node::Node,
        storage::{
            storage_transformer::{StorageTransformerExtension, UsageLogStorageTransformer},
            store,
        },
    };

    use rayon::prelude::{IntoParallelIterator, ParallelIterator};
    use std::sync::Arc;

    // Create a store
    // let path = tempfile::TempDir::new()?;
    // let store = Arc::new(store::FilesystemStore::new(path.path())?);
    // let store = Arc::new(store::FilesystemStore::new("tests/data/sharded_array_write_read.zarr")?);
    let store = Arc::new(store::MemoryStore::default());
    let log_writer = Arc::new(std::sync::Mutex::new(
        // std::io::BufWriter::new(
        std::io::stdout(),
        //    )
    ));
    let usage_log = UsageLogStorageTransformer::new(log_writer, || {
        chrono::Utc::now().format("[%T%.3f] ").to_string()
    });
    let store_readable_listable = usage_log.create_readable_listable_transformer(store.clone());
    let store = usage_log.create_readable_writable_transformer(store);

    // Create a group and write metadata to filesystem
    let group_path = "/group";
    let mut group = zarrs::group::GroupBuilder::new().build(store.clone(), group_path)?;

    // Update group metadata
    group
        .attributes_mut()
        .insert("foo".into(), serde_json::Value::String("bar".into()));

    // Write group metadata to store
    group.store_metadata()?;

    // Create an array
    let array_path = "/group/array";
    let shard_shape = vec![4, 8];
    let inner_chunk_shape = vec![4, 4];
    let mut sharding_codec_builder = ShardingCodecBuilder::new(inner_chunk_shape.clone());
    sharding_codec_builder.bytes_to_bytes_codecs(vec![
        #[cfg(feature = "gzip")]
        Box::new(codec::GzipCodec::new(5)?),
    ]);
    let array = zarrs::array::ArrayBuilder::new(
        vec![8, 8], // array shape
        DataType::UInt16,
        shard_shape.into(),
        FillValue::from(0u16),
    )
    .array_to_bytes_codec(Box::new(sharding_codec_builder.build()))
    .dimension_names(Some(vec!["y".into(), "x".into()]))
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
                chunk_shape.iter().map(|u| *u as usize).collect::<Vec<_>>(),
                |ij| {
                    (s * chunk_shape[0] * chunk_shape[1]
                        + ij[0] as u64 * chunk_shape[1]
                        + ij[1] as u64) as u16
                },
            );
            array.store_chunk_ndarray(&chunk_indices, &chunk_array.view())
        } else {
            Err(zarrs::array::ArrayError::InvalidChunkGridIndicesError(
                chunk_indices.to_vec(),
            ))
        }
    })?;

    // Read the whole array
    let subset_all = ArraySubset::new_with_start_shape(vec![0, 0], array.shape().to_vec())?; // the center 4x2 region
    let data_all = array.retrieve_array_subset_ndarray::<u16>(&subset_all)?;
    println!("The whole array is:\n{:?}\n", data_all);

    // Read a shard back from the store
    let shard_indices = vec![1, 0];
    let data_shard = array.retrieve_chunk_ndarray::<u16>(&shard_indices)?;
    println!("Shard [1,0] is:\n{data_shard:?}\n");

    // Read an inner chunk from the store
    let subset_chunk_1_0 = ArraySubset::new_with_start_shape(vec![4, 0], vec![4, 4])?;
    let data_chunk = array.retrieve_array_subset_ndarray::<u16>(&subset_chunk_1_0)?;
    println!("Chunk [1,0] is:\n{data_chunk:?}\n");

    // Read the central 4x2 subset of the array
    let subset_4x2 = ArraySubset::new_with_start_shape(vec![2, 3], vec![4, 2])?; // the center 4x2 region
    let data_4x2 = array.retrieve_array_subset_ndarray::<u16>(&subset_4x2)?;
    println!("The middle 4x2 subset is:\n{:?}\n", data_4x2);

    // Decode inner chunks
    // In some cases, it might be preferable to decode inner chunks in a shard directly.
    // If using the partial decoder, then the shard index will only be read once from the store.
    let partial_decoder = array.partial_decoder(&[0, 0])?;
    let inner_chunks_to_decode = vec![
        ArraySubset::new_with_start_shape(vec![0, 0], inner_chunk_shape.clone())?,
        ArraySubset::new_with_start_shape(vec![0, 4], inner_chunk_shape.clone())?,
    ];
    let decoded_inner_chunks = partial_decoder.par_partial_decode(&inner_chunks_to_decode)?;
    let decoded_inner_chunks = decoded_inner_chunks
        .into_iter()
        .map(|bytes| {
            let elements = safe_transmute::transmute_many_permissive::<u16>(&bytes)
                .unwrap()
                .to_vec();
            ndarray::ArrayD::<u16>::from_shape_vec(
                inner_chunk_shape
                    .iter()
                    .map(|u| *u as usize)
                    .collect::<Vec<_>>(),
                elements,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    println!("Decoded inner chunks:");
    for (inner_chunk_subset, decoded_inner_chunk) in
        std::iter::zip(inner_chunks_to_decode, decoded_inner_chunks)
    {
        println!("{inner_chunk_subset:?}\n{decoded_inner_chunk:?}\n");
    }

    // Show the hierarchy
    let node = Node::new_with_store(&*store_readable_listable, "/").unwrap();
    let tree = node.hierarchy_tree();
    println!("The zarr hierarchy tree is:\n{}", tree);

    Ok(())
}

fn main() {
    if let Err(err) = sharded_array_write_read() {
        println!("{}", err);
    }
}
