use std::sync::Arc;

use crate::{
    array::{
        codec::{
            options::CodecOptions, ArrayBytes, ArrayCodecTraits, ArrayPartialDecoderTraits,
            ArrayToArrayCodecTraits, CodecError, CodecTraits, RecommendedConcurrency,
        },
        ArrayMetadataOptions, ChunkRepresentation, DataType,
    },
    config::global_config,
    metadata::v3::MetadataV3,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncArrayPartialDecoderTraits;

use super::{
    bitround_partial_decoder, round_bytes, BitroundCodecConfiguration,
    BitroundCodecConfigurationV1, IDENTIFIER,
};

/// A `bitround` codec implementation.
#[derive(Clone, Debug, Default)]
pub struct BitroundCodec {
    keepbits: u32,
}

impl BitroundCodec {
    /// Create a new `bitround` codec.
    ///
    /// `keepbits` is the number of bits to round to in the floating point mantissa.
    #[must_use]
    pub const fn new(keepbits: u32) -> Self {
        Self { keepbits }
    }

    /// Create a new `bitround` codec from a configuration.
    #[must_use]
    pub const fn new_with_configuration(configuration: &BitroundCodecConfiguration) -> Self {
        let BitroundCodecConfiguration::V1(configuration) = configuration;
        Self {
            keepbits: configuration.keepbits,
        }
    }
}

impl CodecTraits for BitroundCodec {
    fn create_metadata_opt(&self, options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        if options.experimental_codec_store_metadata_if_encode_only() {
            let configuration = BitroundCodecConfigurationV1 {
                keepbits: self.keepbits,
            };
            Some(
                MetadataV3::new_with_serializable_configuration(
                    global_config()
                        .experimental_codec_names()
                        .get(super::IDENTIFIER)
                        .expect("experimental codec identifier in global map"),
                    &configuration,
                )
                .unwrap(),
            )
        } else {
            None
        }
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

impl ArrayCodecTraits for BitroundCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // TODO: bitround is well suited to multithread, when is it optimal to kick in?
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToArrayCodecTraits for BitroundCodec {
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        let mut bytes = bytes.into_fixed()?;
        round_bytes(
            bytes.to_mut(),
            decoded_representation.data_type(),
            self.keepbits,
        )?;
        Ok(ArrayBytes::from(bytes))
    }

    fn decode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        _decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        Ok(bytes)
    }

    fn partial_decoder<'a>(
        &'a self,
        input_handle: Arc<dyn ArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(
            bitround_partial_decoder::BitroundPartialDecoder::new(
                input_handle,
                decoded_representation.data_type(),
                self.keepbits,
            )?,
        ))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Arc<dyn AsyncArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(
            bitround_partial_decoder::AsyncBitroundPartialDecoder::new(
                input_handle,
                decoded_representation.data_type(),
                self.keepbits,
            )?,
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<ChunkRepresentation, CodecError> {
        let data_type = decoded_representation.data_type();
        match data_type {
            DataType::Float16
            | DataType::BFloat16
            | DataType::Float32
            | DataType::Float64
            | DataType::UInt8
            | DataType::Int8
            | DataType::UInt16
            | DataType::Int16
            | DataType::UInt32
            | DataType::Int32
            | DataType::UInt64
            | DataType::Int64
            | DataType::Complex64
            | DataType::Complex128 => Ok(decoded_representation.clone()),
            _ => Err(CodecError::UnsupportedDataType(
                data_type.clone(),
                IDENTIFIER.to_string(),
            )),
        }
    }
}
