//! The `vlen_interleaved` array to bytes codec.

mod vlen_interleaved_codec;
mod vlen_interleaved_partial_decoder;

use std::mem::size_of;

pub use vlen_interleaved::IDENTIFIER;

pub use crate::metadata::v3::codec::vlen_interleaved::{
    VlenInterleavedCodecConfiguration, VlenInterleavedCodecConfigurationV1,
};
use crate::{
    array::{codec::CodecError, RawBytes},
    metadata::v3::codec::vlen_interleaved,
};

pub use vlen_interleaved_codec::VlenInterleavedCodec;

use crate::{
    array::codec::{Codec, CodecPlugin},
    metadata::MetadataV3,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_vlen_interleaved, create_codec_vlen_interleaved)
}

fn is_name_vlen_interleaved(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_codec_vlen_interleaved(
    metadata: &MetadataV3,
) -> Result<Codec, PluginCreateError> {
    let configuration: VlenInterleavedCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(VlenInterleavedCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToBytes(codec))
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
