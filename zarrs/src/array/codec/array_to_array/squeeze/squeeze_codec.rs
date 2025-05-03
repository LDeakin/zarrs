use std::{num::NonZeroU64, sync::Arc};

use crate::array::{DataType, FillValue};
use zarrs_metadata::v3::MetadataConfiguration;
use zarrs_registry::codec::SQUEEZE;

use crate::{
    array::{
        codec::{
            ArrayBytes, ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToArrayCodecTraits,
            CodecError, CodecMetadataOptions, CodecOptions, CodecTraits, RecommendedConcurrency,
        },
        ChunkRepresentation, ChunkShape,
    },
    metadata::codec::squeeze::{SqueezeCodecConfiguration, SqueezeCodecConfigurationV1},
    plugin::PluginCreateError,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncArrayPartialDecoderTraits;

/// A Squeeze codec implementation.
#[derive(Clone, Debug)]
pub struct SqueezeCodec {}

impl SqueezeCodec {
    /// Create a new squeeze codec from configuration.
    ///
    /// # Errors
    /// Returns [`PluginCreateError`] if there is a configuration issue.
    pub fn new_with_configuration(
        configuration: &SqueezeCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        match configuration {
            SqueezeCodecConfiguration::V1(_configuration) => Ok(Self::new()),
            _ => Err(PluginCreateError::Other(
                "this squeeze codec configuration variant is unsupported".to_string(),
            )),
        }
    }

    /// Create a new squeeze codec.
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

impl CodecTraits for SqueezeCodec {
    fn identifier(&self) -> &str {
        SQUEEZE
    }

    fn configuration_opt(
        &self,
        _name: &str,
        _options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        let configuration = SqueezeCodecConfiguration::V1(SqueezeCodecConfigurationV1 {});
        Some(configuration.into())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToArrayCodecTraits for SqueezeCodec {
    fn into_dyn(self: Arc<Self>) -> Arc<dyn ArrayToArrayCodecTraits> {
        self as Arc<dyn ArrayToArrayCodecTraits>
    }

    fn encoded_data_type(&self, decoded_data_type: &DataType) -> Result<DataType, CodecError> {
        Ok(decoded_data_type.clone())
    }

    fn encoded_fill_value(
        &self,
        _decoded_data_type: &DataType,
        decoded_fill_value: &FillValue,
    ) -> Result<FillValue, CodecError> {
        Ok(decoded_fill_value.clone())
    }

    fn encoded_shape(&self, decoded_shape: &[NonZeroU64]) -> Result<ChunkShape, CodecError> {
        let encoded_shape: Vec<_> = decoded_shape
            .iter()
            .filter(|dim| dim.get() > 1)
            .copied()
            .collect();
        if encoded_shape.is_empty() {
            Ok(vec![NonZeroU64::new(1).unwrap()].into())
        } else {
            Ok(encoded_shape.into())
        }
    }

    fn decoded_shape(
        &self,
        _encoded_shape: &[NonZeroU64],
    ) -> Result<Option<ChunkShape>, CodecError> {
        Ok(None)
    }

    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        _decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        Ok(bytes)
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
            super::squeeze_partial_decoder::SqueezePartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
            ),
        ))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn AsyncArrayPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(
            super::squeeze_partial_decoder::AsyncSqueezePartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
            ),
        ))
    }
}

impl ArrayCodecTraits for SqueezeCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}
