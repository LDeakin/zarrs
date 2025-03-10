use std::sync::Arc;

use zarrs_plugin::{MetadataConfiguration, PluginCreateError};

use crate::array::{
    codec::{
        array_to_bytes::vlen_v2::IDENTIFIER, ArrayBytes, ArrayCodecTraits,
        ArrayPartialDecoderTraits, ArrayPartialEncoderTraits, ArrayToArrayCodecTraits,
        ArrayToArrayPartialEncoderDefault, CodecError, CodecMetadataOptions, CodecOptions,
        CodecTraits, RecommendedConcurrency,
    },
    ChunkRepresentation, ChunkShape, DataType,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncArrayPartialDecoderTraits;

use super::{
    bitround_partial_decoder, round_bytes, BitroundCodecConfiguration, BitroundCodecConfigurationV1,
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
    ///
    /// # Errors
    /// Returns an error if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &BitroundCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        match configuration {
            BitroundCodecConfiguration::V1(configuration) => Ok(Self {
                keepbits: configuration.keepbits,
            }),
            _ => Err(PluginCreateError::Other(
                "this bitround codec configuration variant is unsupported".to_string(),
            )),
        }
    }
}

impl CodecTraits for BitroundCodec {
    fn identifier(&self) -> &str {
        super::IDENTIFIER
    }

    fn configuration_opt(
        &self,
        _name: &str,
        options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        if options.experimental_codec_store_metadata_if_encode_only() {
            let configuration = BitroundCodecConfiguration::V1(BitroundCodecConfigurationV1 {
                keepbits: self.keepbits,
            });
            Some(configuration.into())
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
    fn dynamic(self: Arc<Self>) -> Arc<dyn ArrayToArrayCodecTraits> {
        self as Arc<dyn ArrayToArrayCodecTraits>
    }

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

    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn ArrayPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(
            bitround_partial_decoder::BitroundPartialDecoder::new(
                input_handle,
                decoded_representation.data_type(),
                self.keepbits,
            )?,
        ))
    }

    fn partial_encoder(
        self: Arc<Self>,
        input_handle: Arc<dyn ArrayPartialDecoderTraits>,
        output_handle: Arc<dyn ArrayPartialEncoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialEncoderTraits>, CodecError> {
        Ok(Arc::new(ArrayToArrayPartialEncoderDefault::new(
            input_handle,
            output_handle,
            decoded_representation.clone(),
            self,
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn AsyncArrayPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits>, CodecError> {
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

    fn compute_decoded_shape(&self, encoded_shape: ChunkShape) -> Result<ChunkShape, CodecError> {
        Ok(encoded_shape)
    }
}
