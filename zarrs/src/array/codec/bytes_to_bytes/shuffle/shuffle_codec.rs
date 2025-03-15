use std::{borrow::Cow, sync::Arc};

use zarrs_plugin::{MetadataConfiguration, PluginCreateError};

use crate::array::{
    codec::{
        bytes_partial_decoder_default_sync::BytesPartialDecoderDefault, BytesPartialDecoderTraits,
        BytesPartialEncoderDefault, BytesPartialEncoderTraits, BytesToBytesCodecTraits, CodecError,
        CodecMetadataOptions, CodecOptions, CodecTraits, RecommendedConcurrency,
    },
    BytesRepresentation, RawBytes,
};

#[cfg(feature = "async")]
use crate::array::codec::bytes_partial_decoder_default_async::AsyncBytesPartialDecoderDefault;
#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{ShuffleCodecConfiguration, ShuffleCodecConfigurationV1};

/// A `shuffle` codec implementation.
#[derive(Clone, Debug, Default)]
pub struct ShuffleCodec {
    elementsize: usize,
}

impl ShuffleCodec {
    /// Create a new `shuffle` codec.
    #[must_use]
    pub fn new(elementsize: usize) -> Self {
        Self { elementsize }
    }

    /// Create a new `shuffle` codec.
    ///
    /// # Errors
    /// Returns an error if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &ShuffleCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        match configuration {
            ShuffleCodecConfiguration::V1(configuration) => Ok(Self {
                elementsize: configuration.elementsize,
            }),
            _ => Err(PluginCreateError::Other(
                "this shuffle codec configuration variant is unsupported".to_string(),
            )),
        }
    }
}

impl CodecTraits for ShuffleCodec {
    fn identifier(&self) -> &str {
        super::IDENTIFIER
    }

    fn configuration_opt(
        &self,
        _name: &str,
        _options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        let configuration = ShuffleCodecConfiguration::V1(ShuffleCodecConfigurationV1 {
            elementsize: self.elementsize,
        });
        Some(configuration.into())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

fn is_multiple_of(lhs: usize, rhs: usize) -> bool {
    match rhs {
        // prevent division by zero
        0 => lhs == 0,
        _ => lhs % rhs == 0,
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl BytesToBytesCodecTraits for ShuffleCodec {
    fn into_dyn(self: Arc<Self>) -> Arc<dyn BytesToBytesCodecTraits> {
        self as Arc<dyn BytesToBytesCodecTraits>
    }

    fn recommended_concurrency(
        &self,
        _decoded_representation: &BytesRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        Ok(RecommendedConcurrency::new_maximum(1))
    }

    fn encode<'a>(
        &self,
        decoded_value: RawBytes<'a>,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        if !is_multiple_of(decoded_value.len(), self.elementsize) {
            return Err(CodecError::Other("the shuffle codec expects the input byte length to be an integer multiple of the elementsize".to_string()));
        }

        let mut encoded_value = decoded_value.to_vec();
        let count = encoded_value.len().div_ceil(self.elementsize);
        for i in 0..count {
            let offset = i * self.elementsize;
            for byte_index in 0..self.elementsize {
                let j = byte_index * count + i;
                encoded_value[j] = decoded_value[offset + byte_index];
            }
        }
        Ok(Cow::Owned(encoded_value))
    }

    fn decode<'a>(
        &self,
        encoded_value: RawBytes<'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        if !is_multiple_of(encoded_value.len(), self.elementsize) {
            return Err(CodecError::Other("the shuffle codec expects the input byte length to be an integer multiple of the elementsize".to_string()));
        }

        let mut decoded_value = encoded_value.to_vec();
        let count = decoded_value.len().div_ceil(self.elementsize);
        for i in 0..self.elementsize {
            let offset = i * count;
            for byte_index in 0..count {
                let j = byte_index * self.elementsize + i;
                decoded_value[j] = encoded_value[offset + byte_index];
            }
        }
        Ok(Cow::Owned(decoded_value))
    }

    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn BytesPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(BytesPartialDecoderDefault::new(
            input_handle,
            *decoded_representation,
            self.clone(),
        )))
    }

    fn partial_encoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn BytesPartialEncoderTraits>, CodecError> {
        Ok(Arc::new(BytesPartialEncoderDefault::new(
            input_handle,
            output_handle,
            *decoded_representation,
            self,
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncBytesPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(AsyncBytesPartialDecoderDefault::new(
            input_handle,
            *decoded_representation,
            self.clone(),
        )))
    }

    fn encoded_representation(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        *decoded_representation
    }
}
