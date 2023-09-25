#[cfg(all(feature = "ndarray", feature = "sharding"))]
fn sharded_array_write_read() -> Result<(), Box<dyn std::error::Error>> {
    use zarrs::{
        array::{
            codec::{self, ShardingCodec},
            CodecChain, DataType, FillValue,
        },
        array_subset::ArraySubset,
        node::Node,
        storage::store,
    };

    use rayon::prelude::{IntoParallelIterator, ParallelIterator};
    use std::sync::Arc;

    // Create a store
    // let path = tempdir::TempDir::new("example")?;
    // let store = Arc::new(store::filesystem::FilesystemStore::new(path.path())?);
    let store = Arc::new(store::MemoryStore::default());

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
    let inner_codecs = CodecChain::new(
        vec![],
        Box::new(codec::BytesCodec::little()),
        vec![
            #[cfg(feature = "gzip")]
            Box::new(codec::GzipCodec::new(5)?),
        ],
    );
    let index_codecs = CodecChain::new(
        vec![],
        Box::new(codec::BytesCodec::little()),
        vec![
            #[cfg(feature = "crc32c")]
            Box::new(codec::Crc32cCodec::new()),
        ],
    );
    let array = zarrs::array::ArrayBuilder::new(
        vec![8, 8], // array shape
        DataType::UInt16,
        vec![4, 8].into(), // shard shape,
        FillValue::from(0u16),
    )
    .array_to_bytes_codec(Box::new(ShardingCodec::new(
        vec![4, 4], // inner chunk shape
        inner_codecs,
        index_codecs,
    )))
    .dimension_names(vec!["y".into(), "x".into()])
    .storage_transformers(vec![])
    .build(store.clone(), array_path)?;

    // Write array metadata to store
    array.store_metadata()?;

    // Write some shards (in parallel)
    (0..2)
        .into_par_iter()
        .map(|s| {
            let chunk_grid = array.chunk_grid();
            let chunk_indices = vec![s, 0];
            let chunk_shape = chunk_grid.chunk_shape(&chunk_indices, &array.shape())?;
            let chunk_array = ndarray::ArrayD::<u16>::from_shape_fn(chunk_shape.clone(), |ij| {
                (s * chunk_shape[0] * chunk_shape[1] + ij[0] * chunk_shape[1] + ij[1]) as u16
            });
            array.store_chunk_ndarray(&chunk_indices, &chunk_array.view())
        })
        .collect::<Vec<_>>();

    // The array metadata is
    println!(
        "The array metadata is:\n{}\n",
        serde_json::to_string_pretty(&array.metadata()).unwrap()
    );

    // Read the whole array
    let subset_all = ArraySubset::new_with_start_shape(vec![0, 0], array.shape().to_vec())?; // the center 2x2 region
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

    // Read the central 2x2 subset of the array
    let subset_2x2 = ArraySubset::new_with_start_shape(vec![3, 3], vec![2, 2])?; // the center 2x2 region
    let data_2x2 = array.retrieve_array_subset_ndarray::<u16>(&subset_2x2)?;
    println!("The middle 2x2 subset is:\n{:?}\n", data_2x2);

    // Show the hierarchy
    let node = Node::new_with_store(&store, "/").unwrap();
    let tree = node.hierarchy_tree();
    println!("The zarr hierarchy tree is:\n{}", tree);

    Ok(())
}

#[cfg(any(not(feature = "ndarray"), not(feature = "sharding")))]
fn sharded_array_write_read() -> Result<(), Box<dyn std::error::Error>> {
    panic!("the sharded_array_write_read example requires the ndarray and sharding feature")
}

fn main() {
    if let Err(err) = sharded_array_write_read() {
        println!("{}", err);
    }
}
