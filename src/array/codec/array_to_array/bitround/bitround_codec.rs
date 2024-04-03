use crate::{
    array::{
        codec::{
            options::CodecOptions, ArrayCodecTraits, ArrayPartialDecoderTraits,
            ArrayToArrayCodecTraits, CodecError, CodecTraits, RecommendedConcurrency,
        },
        ArrayMetadataOptions, ChunkRepresentation, DataType,
    },
    metadata::Metadata,
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
    fn create_metadata_opt(&self, options: &ArrayMetadataOptions) -> Option<Metadata> {
        if options.experimental_codec_store_metadata_if_encode_only() {
            let configuration = BitroundCodecConfigurationV1 {
                keepbits: self.keepbits,
            };
            Some(Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
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

    fn encode(
        &self,
        mut decoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Vec<u8>, CodecError> {
        round_bytes(
            &mut decoded_value,
            decoded_representation.data_type(),
            self.keepbits,
        )?;
        Ok(decoded_value)
    }

    fn decode(
        &self,
        encoded_value: Vec<u8>,
        _decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Vec<u8>, CodecError> {
        Ok(encoded_value)
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToArrayCodecTraits for BitroundCodec {
    fn partial_decoder<'a>(
        &'a self,
        input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
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
        input_handle: Box<dyn AsyncArrayPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
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
            DataType::Float16 | DataType::BFloat16 | DataType::Float32 | DataType::Float64 => {
                Ok(decoded_representation.clone())
            }
            _ => Err(CodecError::UnsupportedDataType(
                data_type.clone(),
                IDENTIFIER.to_string(),
            )),
        }
    }
}
