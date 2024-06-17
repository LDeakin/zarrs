use std::mem::size_of;

use itertools::Itertools;

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, CodecError, CodecOptions, CodecTraits,
            RecommendedConcurrency,
        },
        ArrayBytes, ArrayMetadataOptions, BytesRepresentation, ChunkRepresentation, DataTypeSize,
        RawBytes,
    },
    metadata::v3::MetadataV3,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{VlenInterleavedCodecConfiguration, VlenInterleavedCodecConfigurationV1};

/// The `vlen_interleaved` codec implementation.
#[derive(Debug, Clone, Default)]
pub struct VlenInterleavedCodec {}

impl VlenInterleavedCodec {
    /// Create a new `vlen` codec.
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    /// Create a new `vlen` codec from configuration.
    #[must_use]
    pub fn new_with_configuration(_configuration: &VlenInterleavedCodecConfiguration) -> Self {
        // let VlenInterleavedCodecConfiguration::V1(configuration) = configuration;
        Self {}
    }
}

impl CodecTraits for VlenInterleavedCodec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let configuration = VlenInterleavedCodecConfigurationV1 {};
        Some(
            MetadataV3::new_with_serializable_configuration(super::IDENTIFIER, &configuration)
                .unwrap(),
        )
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true // TODO: Vlen could do partial decoding, but needs coalescing etc
    }
}

impl ArrayCodecTraits for VlenInterleavedCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for VlenInterleavedCodec {
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        bytes.validate(
            decoded_representation.num_elements(),
            decoded_representation.data_type().size(),
        )?;
        let (bytes, offsets) = bytes.into_variable()?;

        let num_elements = decoded_representation.num_elements();
        debug_assert_eq!(1 + num_elements, offsets.len() as u64);

        let mut data: Vec<u8> = Vec::with_capacity(offsets.len() * size_of::<u32>() + bytes.len());
        // Number of elements
        let num_elements = u32::try_from(num_elements).map_err(|_| {
            CodecError::Other("num_elements exceeds u32::MAX in vlen codec".to_string())
        })?;
        data.extend_from_slice(num_elements.to_le_bytes().as_slice());
        // Interleaved length (u32, little endian) and element bytes
        for (&curr, &next) in offsets.iter().tuple_windows() {
            let element_bytes = &bytes[curr..next];
            let element_bytes_len = u32::try_from(element_bytes.len()).unwrap();
            data.extend_from_slice(&element_bytes_len.to_le_bytes());
            data.extend_from_slice(element_bytes);
        }

        Ok(data.into())
    }

    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        let num_elements = decoded_representation.num_elements_usize();
        let (bytes, offsets) = super::get_interleaved_bytes_and_offsets(num_elements, &bytes)?;
        Ok(ArrayBytes::new_vlen(bytes, offsets))
    }

    fn partial_decoder<'a>(
        &self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            super::vlen_interleaved_partial_decoder::VlenInterleavedPartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
            ),
        ))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            super::vlen_interleaved_partial_decoder::AsyncVlenInterleavedPartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
            ),
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        match decoded_representation.data_type().size() {
            DataTypeSize::Variable => Ok(BytesRepresentation::UnboundedSize),
            DataTypeSize::Fixed(_) => {
                return Err(CodecError::UnsupportedDataType(
                    decoded_representation.data_type().clone(),
                    super::IDENTIFIER.to_string(),
                ))
            }
        }
    }
}
