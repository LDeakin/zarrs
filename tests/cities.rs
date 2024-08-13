#![cfg(all(feature = "sharding", feature = "zstd"))]

use std::{
    error::Error,
    fs::File,
    io::{BufRead, BufReader},
};

use zarrs::{
    array::{
        codec::{
            array_to_bytes::{
                sharding::ShardingCodecBuilder, vlen::VlenCodec, vlen_v2::VlenV2Codec,
            },
            ArrayToBytesCodecTraits, ZstdCodec,
        },
        ArrayBuilder, ArrayMetadataOptions, DataType, FillValue,
    },
    array_subset::ArraySubset,
    metadata::v3::codec::vlen::VlenCodecConfiguration,
    storage::{
        store::{FilesystemStore, MemoryStore},
        ReadableWritableListableStorage,
    },
};

fn read_cities() -> std::io::Result<Vec<String>> {
    let reader = BufReader::new(File::open("tests/data/cities.csv")?);
    let mut cities = Vec::with_capacity(47868);
    for line in reader.lines() {
        cities.push(line?);
    }
    Ok(cities)
}

fn cities_impl(
    cities: &[String],
    compression_level: Option<i32>,
    chunk_size: u64,
    shard_size: Option<u64>,
    vlen_codec: Box<dyn ArrayToBytesCodecTraits>,
    write_to_file: bool,
) -> Result<u64, Box<dyn Error>> {
    let store: ReadableWritableListableStorage = if write_to_file {
        std::sync::Arc::new(FilesystemStore::new("tests/data/v3/cities.zarr")?)
    } else {
        std::sync::Arc::new(MemoryStore::default())
    };
    store.erase_prefix(&"".try_into().unwrap())?;

    let mut builder = ArrayBuilder::new(
        vec![cities.len() as u64], // array shape
        DataType::String,
        vec![chunk_size].try_into()?, // regular chunk shape
        FillValue::from(""),
    );
    if let Some(shard_size) = shard_size {
        builder.array_to_bytes_codec(Box::new(
            ShardingCodecBuilder::new(
                vec![shard_size].try_into()?, // inner chunk chape
            )
            .array_to_bytes_codec(vlen_codec)
            .build(),
        ));
    } else {
        builder.array_to_bytes_codec(vlen_codec);
    }
    if let Some(compression_level) = compression_level {
        builder.bytes_to_bytes_codecs(vec![
            #[cfg(feature = "zstd")]
            Box::new(ZstdCodec::new(compression_level, false)),
        ]);
    }

    let array = builder.build(store.clone(), "/")?;
    array.store_metadata_opt(&ArrayMetadataOptions::default().set_include_zarrs_metadata(false))?;

    let subset_all = ArraySubset::new_with_shape(array.shape().to_vec());
    array.store_array_subset_elements(&subset_all, &cities)?;
    let cities_out = array.retrieve_array_subset_elements::<String>(&subset_all)?;
    assert_eq!(cities, cities_out);

    let last_block = array.retrieve_chunk(&[(cities.len() as u64).div_ceil(chunk_size)])?;
    let (_bytes, offsets) = last_block.into_variable()?;
    assert_eq!(offsets.len() as u64, chunk_size + 1);

    Ok(store.size_prefix(&"c/".try_into().unwrap())?) // only chunks
}

#[rustfmt::skip]
#[test]
fn cities() -> Result<(), Box<dyn Error>> {
    let cities = read_cities()?;
    assert_eq!(cities.len(), 47868);
    assert_eq!(cities[0], "Tokyo");
    assert_eq!(cities[47862], "Sariwŏn-si");
    assert_eq!(cities[47867], "Charlotte Amalie");

    let vlen_v2 = Box::new(VlenV2Codec::default());

    // let vlen = Box::new(VlenCodec::default());
    let vlen_configuration: VlenCodecConfiguration = serde_json::from_str(r#"{
        "data_codecs": [{"name": "bytes"}],
        "index_codecs": [{"name": "bytes","configuration": { "endian": "little" }}],
        "index_data_type": "uint32"
    }"#)?;
    let vlen = Box::new(VlenCodec::new_with_configuration(&vlen_configuration)?);

    let vlen_compressed_configuration: VlenCodecConfiguration = serde_json::from_str(r#"{
        "data_codecs": [{"name": "bytes"},{"name": "blosc","configuration": {"cname": "zstd", "clevel":5,"shuffle": "bitshuffle", "typesize":1,"blocksize":0}}],
        "index_codecs": [{"name": "bytes","configuration": { "endian": "little" }},{"name": "blosc","configuration":{"cname": "zstd", "clevel":5,"shuffle": "shuffle", "typesize":4,"blocksize":0}}],
        "index_data_type": "uint32"
    }"#)?;
    let vlen_compressed = Box::new(VlenCodec::new_with_configuration(&vlen_compressed_configuration)?);

    print!("| encoding         | compression | size   |\n");
    print!("| ---------------- | ----------- | ------ |\n");
    print!("| vlen_v2 |             | {} |\n", cities_impl(&cities, None, 1000, None, vlen_v2.clone(), true)?);
    print!("| vlen_v2 | zstd 5      | {} |\n", cities_impl(&cities, Some(5), 1000, None, vlen_v2.clone(), false)?);
    print!("| vlen             |             | {} |\n", cities_impl(&cities, None, 1000, None, vlen.clone(), false)?);
    print!("| vlen             | zstd 5      | {} |\n", cities_impl(&cities, None, 1000, None, vlen_compressed.clone(), false)?);
    println!();
    // panic!();

    // | encoding         | compression | size   |
    // | ---------------- | ----------- | ------ |
    // | vlen_v2 |             | 642196 |
    // | vlen_v2 | zstd 5      | 362626 |
    // | vlen             |             | 642580 |
    // | vlen             | zstd 5      | 346950 |

    Ok(())
}
