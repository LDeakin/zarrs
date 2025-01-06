#![allow(missing_docs)]

use std::sync::Arc;

use zarrs::array::ArrayMetadataOptions;
use zarrs::config::MetadataConvertVersion;
use zarrs::group::{Group, GroupMetadataOptions};
use zarrs::metadata::v2::ArrayMetadataV2;
use zarrs_metadata::v2::array::{ArrayMetadataV2Order, FillValueMetadataV2};
use zarrs_metadata::v2::GroupMetadataV2;
use zarrs_metadata::{ChunkKeySeparator, GroupMetadata};
use zarrs_storage::{ListableStorageTraits, ReadableStorageTraits, StoreKey};

fn key_to_str<T: ReadableStorageTraits>(
    store: &Arc<T>,
    key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(String::from_utf8(
        store.get(&StoreKey::new(key)?)?.unwrap().into(),
    )?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(zarrs_storage::store::MemoryStore::new());

    let serde_json::Value::Object(attributes) = serde_json::json!({
        "foo": "bar",
        "baz": 42,
    }) else {
        unreachable!()
    };

    // Create a Zarr V2 group
    let group_metadata: GroupMetadata = GroupMetadataV2::new()
        .with_attributes(attributes.clone())
        .into();
    let group = Group::new_with_metadata(store.clone(), "/group", group_metadata)?;

    // Store the metadata as V2 and V3
    let convert_group_metadata_to_v3 =
        GroupMetadataOptions::default().with_metadata_convert_version(MetadataConvertVersion::V3);
    group.store_metadata()?;
    group.store_metadata_opt(&convert_group_metadata_to_v3)?;
    println!(
        "group/.zgroup (Zarr V2 group metadata):\n{}\n",
        key_to_str(&store, "group/.zgroup")?
    );
    println!(
        "group/.zattrs (Zarr V2 group attributes):\n{}\n",
        key_to_str(&store, "group/.zattrs")?
    );
    println!(
        "group/zarr.json (Zarr V3 equivalent group metadata/attributes):\n{}\n",
        key_to_str(&store, "group/zarr.json")?
    );
    // println!(
    //     "The equivalent Zarr V3 group metadata is\n{}\n",
    //     serde_json::to_string_pretty(&group.metadata_opt(&convert_group_metadata_to_v3))?
    // );

    // Create a Zarr V2 array
    let array_metadata = ArrayMetadataV2::new(
        vec![10, 10],
        vec![5, 5].try_into()?,
        ">f4".into(), // big endian float32
        FillValueMetadataV2::NaN,
        None,
        None,
    )
    .with_dimension_separator(ChunkKeySeparator::Slash)
    .with_order(ArrayMetadataV2Order::F)
    .with_attributes(attributes.clone());
    let array = zarrs::array::Array::new_with_metadata(
        store.clone(),
        "/group/array",
        array_metadata.into(),
    )?;

    // Store the metadata as V2 and V3
    let convert_array_metadata_to_v3 =
        ArrayMetadataOptions::default().with_metadata_convert_version(MetadataConvertVersion::V3);
    array.store_metadata()?;
    array.store_metadata_opt(&convert_array_metadata_to_v3)?;
    println!(
        "group/array/.zarray (Zarr V2 array metadata):\n{}\n",
        key_to_str(&store, "group/array/.zarray")?
    );
    println!(
        "group/array/.zattrs (Zarr V2 array attributes):\n{}\n",
        key_to_str(&store, "group/array/.zattrs")?
    );
    println!(
        "group/array/zarr.json (Zarr V3 equivalent array metadata/attributes):\n{}\n",
        key_to_str(&store, "group/array/zarr.json")?
    );
    // println!(
    //     "The equivalent Zarr V3 array metadata is\n{}\n",
    //     serde_json::to_string_pretty(&array.metadata_opt(&convert_array_metadata_to_v3))?
    // );

    array.store_chunk_elements::<f32>(&[0, 1], &[0.0; 5 * 5])?;

    // Print the keys in the store
    println!("The store contains keys:");
    for key in store.list()? {
        println!("  {}", key);
    }

    Ok(())
}
