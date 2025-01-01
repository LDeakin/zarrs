//! The `vlen_v2` array to bytes codec.

mod vlen_v2_codec;
mod vlen_v2_partial_decoder;

pub(crate) mod vlen_v2_macros;

use std::{mem::size_of, sync::Arc};

/// The identifier for the `vlen_v2` codec.
pub(crate) const IDENTIFIER: &str = "vlen_v2";
// pub use vlen_v2::IDENTIFIER;

use crate::array::{codec::CodecError, RawBytes};

pub(crate) use vlen_v2_codec::VlenV2Codec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::v3::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_vlen_v2, create_codec_vlen_v2)
}
inventory::submit! {
    CodecPlugin::new(crate::metadata::v2::array::codec::vlen_array::IDENTIFIER, is_name_vlen_array, create_codec_vlen_v2)
}
inventory::submit! {
    CodecPlugin::new(crate::metadata::v2::array::codec::vlen_bytes::IDENTIFIER, is_name_vlen_bytes, create_codec_vlen_v2)
}
inventory::submit! {
    CodecPlugin::new(crate::metadata::v2::array::codec::vlen_utf8::IDENTIFIER, is_name_vlen_utf8, create_codec_vlen_v2)
}

fn is_name_vlen_v2(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn is_name_vlen_array(name: &str) -> bool {
    name.eq(crate::metadata::v2::array::codec::vlen_array::IDENTIFIER)
}

fn is_name_vlen_bytes(name: &str) -> bool {
    name.eq(crate::metadata::v2::array::codec::vlen_bytes::IDENTIFIER)
}

fn is_name_vlen_utf8(name: &str) -> bool {
    name.eq(crate::metadata::v2::array::codec::vlen_utf8::IDENTIFIER)
}

pub(crate) fn create_codec_vlen_v2(metadata: &MetadataV3) -> Result<Codec, PluginCreateError> {
    if metadata.configuration_is_none_or_empty() {
        let codec = Arc::new(VlenV2Codec::new(metadata.name().to_string()));
        Ok(Codec::ArrayToBytes(codec))
    } else {
        Err(PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()).into())
    }
}

fn get_interleaved_bytes_and_offsets(
    num_elements: usize,
    bytes: &RawBytes,
) -> Result<(Vec<u8>, Vec<usize>), CodecError> {
    // Validate the bytes is long enough to contain header and element lengths
    let header_length = size_of::<u32>() * (1 + num_elements);
    if bytes.len() < header_length {
        return Err(CodecError::UnexpectedChunkDecodedSize(
            bytes.len(),
            header_length as u64,
        ));
    }

    // Validate the number of elements from the header
    let header_num_elements = u32::from_le_bytes((&bytes[0..size_of::<u32>()]).try_into().unwrap());
    if u32::try_from(num_elements).unwrap() != header_num_elements {
        return Err(CodecError::Other(format!(
            "Expected header with {num_elements} elements, got {header_num_elements}"
        )));
    }

    let bytes_len = bytes.len() - header_length;
    let mut bytes_out = Vec::with_capacity(bytes_len);
    let mut offsets_out = Vec::with_capacity(num_elements + 1);
    let mut offset = size_of::<u32>();
    for _element in 0..num_elements {
        let length =
            u32::from_le_bytes(bytes[offset..offset + size_of::<u32>()].try_into().unwrap());
        offset += size_of::<u32>();
        offsets_out.push(bytes_out.len());
        if length != 0 {
            bytes_out.extend_from_slice(&bytes[offset..offset + length as usize]);
            offset += length as usize;
        }
    }
    offsets_out.push(bytes_out.len());

    Ok((bytes_out, offsets_out))
}
