//! The `vlen` array to bytes codec.

mod vlen_codec;
mod vlen_partial_decoder;

use std::{mem::size_of, num::NonZeroU64};

use itertools::Itertools;
pub use vlen::IDENTIFIER;

pub use crate::metadata::v3::codec::vlen::{VlenCodecConfiguration, VlenCodecConfigurationV1};
use crate::{
    array::{
        codec::{ArrayToBytesCodecTraits, CodecError, CodecOptions},
        convert_from_bytes_slice, ChunkRepresentation, CodecChain, DataType, Endianness, FillValue,
        RawBytes, NATIVE_ENDIAN,
    },
    config::global_config,
    metadata::v3::codec::vlen,
};

pub use vlen_codec::VlenCodec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

use super::bytes::reverse_endianness;

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_vlen, create_codec_vlen)
}

fn is_name_vlen(name: &str) -> bool {
    name.eq(IDENTIFIER)
        || name
            == global_config()
                .experimental_codec_names()
                .get(IDENTIFIER)
                .expect("experimental codec identifier in global map")
}

pub(crate) fn create_codec_vlen(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    let configuration: VlenCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(VlenCodec::new_with_configuration(&configuration)?);
    Ok(Codec::ArrayToBytes(codec))
}

fn get_vlen_bytes_and_offsets(
    index_chunk_representation: &ChunkRepresentation,
    bytes: &RawBytes,
    index_codecs: &CodecChain,
    data_codecs: &CodecChain,
    options: &CodecOptions,
) -> Result<(Vec<u8>, Vec<usize>), CodecError> {
    // Get the index length and data start
    if bytes.len() < size_of::<u64>() {
        return Err(CodecError::UnexpectedChunkDecodedSize(
            bytes.len(),
            size_of::<u64>() as u64,
        ));
    }
    let index_len = u64::from_le_bytes(bytes[0..size_of::<u64>()].try_into().unwrap());
    let index_len = usize::try_from(index_len)
        .map_err(|_| CodecError::Other("index length exceeds usize::MAX".to_string()))?;
    let data_start = size_of::<u64>() + index_len;
    let data_compressed_len = bytes.len() - data_start;

    // Decode the index
    let index = &bytes[size_of::<u64>()..data_start];
    let mut index_bytes = index_codecs
        .decode(index.into(), index_chunk_representation, options)?
        .into_fixed()?;
    if NATIVE_ENDIAN == Endianness::Big {
        reverse_endianness(index_bytes.to_mut(), &DataType::UInt64);
    }
    let index = match index_chunk_representation.data_type() {
        // DataType::UInt8 => {
        //     let index = convert_from_bytes_slice::<u8>(&index_bytes);
        //     offsets_u8_to_usize(index)
        // }
        // DataType::UInt16 => {
        //     let index = convert_from_bytes_slice::<u16>(&index_bytes);
        //     offsets_u16_to_usize(index)
        // }
        DataType::UInt32 => {
            let index = convert_from_bytes_slice::<u32>(&index_bytes);
            offsets_u32_to_usize(index)
        }
        DataType::UInt64 => {
            let index = convert_from_bytes_slice::<u64>(&index_bytes);
            offsets_u64_to_usize(index)
        }
        _ => unreachable!("other data types are not part of VlenIndexDataType"),
    };

    // Get the data length
    let Some(&data_len_expected) = index.last() else {
        return Err(CodecError::Other(
            "Index is empty? It should have at least one element".to_string(),
        ));
    };

    // Decode the data
    let data = &bytes[data_start..data_start + data_compressed_len];
    let data = if let Ok(data_len_expected) = NonZeroU64::try_from(data_len_expected as u64) {
        data_codecs.decode(
            data.into(),
            &unsafe {
                // SAFETY: data type and fill value are compatible
                ChunkRepresentation::new_unchecked(
                    vec![data_len_expected],
                    DataType::UInt8,
                    FillValue::from(0u8),
                )
            },
            options,
        )?
    } else {
        vec![].into()
    }
    .into_fixed()?
    .into_owned();
    let data_len = data.len();

    // Check the data length is as expected
    if data_len != data_len_expected {
        return Err(CodecError::Other(format!(
            "Expected data length {data_len_expected} does not match data length {data_len}"
        )));
    }

    // Validate the offsets
    for (curr, next) in index.iter().tuple_windows() {
        if next < curr || *next > data_len {
            return Err(CodecError::Other(
                "Invalid bytes offsets in vlen Offset64 encoded chunk".to_string(),
            ));
        }
    }

    Ok((data, index))
}

// /// Convert u8 offsets to usize
// ///
// /// # Panics if the offsets exceed [`usize::MAX`].
// fn offsets_u8_to_usize(offsets: Vec<u8>) -> Vec<usize> {
//     if size_of::<u8>() == size_of::<usize>() {
//         bytemuck::allocation::cast_vec(offsets)
//     } else {
//         offsets
//             .into_iter()
//             .map(|offset| usize::from(offset))
//             .collect()
//     }
// }

// /// Convert u16 offsets to usize
// ///
// /// # Panics if the offsets exceed [`usize::MAX`].
// fn offsets_u16_to_usize(offsets: Vec<u16>) -> Vec<usize> {
//     if size_of::<u16>() == size_of::<usize>() {
//         bytemuck::allocation::cast_vec(offsets)
//     } else {
//         offsets
//             .into_iter()
//             .map(|offset| usize::from(offset))
//             .collect()
//     }
// }

/// Convert u32 offsets to usize
///
/// # Panics if the offsets exceed [`usize::MAX`].
fn offsets_u32_to_usize(offsets: Vec<u32>) -> Vec<usize> {
    if size_of::<u32>() == size_of::<usize>() {
        bytemuck::allocation::cast_vec(offsets)
    } else {
        offsets
            .into_iter()
            .map(|offset| usize::try_from(offset).unwrap())
            .collect()
    }
}

/// Convert u64 offsets to usize
///
/// # Panics if the offsets exceed [`usize::MAX`].
fn offsets_u64_to_usize(offsets: Vec<u64>) -> Vec<usize> {
    if size_of::<u64>() == size_of::<usize>() {
        bytemuck::allocation::cast_vec(offsets)
    } else {
        offsets
            .into_iter()
            .map(|offset| usize::try_from(offset).unwrap())
            .collect()
    }
}
