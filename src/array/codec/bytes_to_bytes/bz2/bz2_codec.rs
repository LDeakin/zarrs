use std::io::Read;

use crate::{
    array::{
        codec::{
            BytesPartialDecoderTraits, BytesToBytesCodecTraits, CodecError, CodecTraits,
            DecodeOptions, EncodeOptions, PartialDecoderOptions, RecommendedConcurrency,
        },
        BytesRepresentation,
    },
    metadata::Metadata,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{
    bz2_configuration::{Bz2CodecConfiguration, Bz2CodecConfigurationV1},
    bz2_partial_decoder, Bz2CompressionLevel, IDENTIFIER,
};

/// A `bz2` codec implementation.
#[derive(Clone, Debug)]
pub struct Bz2Codec {
    compression: bzip2::Compression,
}

impl Bz2Codec {
    /// Create a new `bz2` codec.
    pub fn new(level: Bz2CompressionLevel) -> Self {
        let compression = bzip2::Compression::new(level.as_u32());
        Self { compression }
    }

    /// Create a new `bz2` codec from configuration.
    ///
    /// # Errors
    /// Returns [`PluginCreateError`] if the configuration is not supported.
    pub fn new_with_configuration(configuration: &Bz2CodecConfiguration) -> Self {
        let Bz2CodecConfiguration::V1(configuration) = configuration;
        Self::new(configuration.level)
    }
}

impl CodecTraits for Bz2Codec {
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = Bz2CodecConfigurationV1 {
            level: Bz2CompressionLevel(self.compression.level()),
        };
        Some(Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl BytesToBytesCodecTraits for Bz2Codec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &BytesRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // bz2 does not support parallel decode
        Ok(RecommendedConcurrency::new_maximum(1))
    }

    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        _options: &EncodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        let mut encoder = bzip2::read::BzEncoder::new(decoded_value.as_slice(), self.compression);
        let mut out: Vec<u8> = Vec::new();
        encoder.read_to_end(&mut out)?;
        Ok(out)
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        _decoded_representation: &BytesRepresentation,
        _options: &DecodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        let mut decoder = bzip2::read::BzDecoder::new(encoded_value.as_slice());
        let mut out: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(out)
    }

    fn partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &PartialDecoderOptions,
    ) -> Result<Box<dyn BytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(bz2_partial_decoder::Bz2PartialDecoder::new(
            input_handle,
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &PartialDecoderOptions,
    ) -> Result<Box<dyn AsyncBytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(bz2_partial_decoder::AsyncBz2PartialDecoder::new(
            input_handle,
        )))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        decoded_representation
            .size()
            .map_or(BytesRepresentation::UnboundedSize, |size| {
                // https://en.wikipedia.org/wiki/Bzip2#Implementation
                // TODO: Below assumes a maximum expansion of 1.25 for the blocks + header (4 byte) + footer (11 byte), but need to read spec
                BytesRepresentation::BoundedSize(4 + 11 + size + (size + 3) / 4)
            })
    }
}
