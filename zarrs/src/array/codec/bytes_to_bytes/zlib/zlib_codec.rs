use std::{
    borrow::Cow,
    io::{Cursor, Read},
    sync::Arc,
};

use zarrs_metadata::codec::ZLIB;
use zarrs_plugin::{MetadataConfiguration, PluginCreateError};

use crate::array::{
    codec::{
        BytesToBytesCodecTraits, CodecError, CodecMetadataOptions, CodecOptions, CodecTraits,
        RecommendedConcurrency,
    },
    BytesRepresentation, RawBytes,
};

use super::{ZlibCodecConfiguration, ZlibCodecConfigurationV1, ZlibCompressionLevel};

/// A `zlib` codec implementation.
#[derive(Clone, Debug)]
pub struct ZlibCodec {
    compression: flate2::Compression,
}

impl ZlibCodec {
    /// Create a new `zlib` codec.
    #[must_use]
    pub fn new(level: ZlibCompressionLevel) -> Self {
        let compression = flate2::Compression::new(level.as_u32());
        Self { compression }
    }

    /// Create a new `zlib` codec from configuration.
    ///
    /// # Errors
    /// Returns an error if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &ZlibCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        match configuration {
            ZlibCodecConfiguration::V1(configuration) => Ok(Self::new(configuration.level)),
            _ => Err(PluginCreateError::Other(
                "this zlib codec configuration variant is unsupported".to_string(),
            )),
        }
    }
}

impl CodecTraits for ZlibCodec {
    fn identifier(&self) -> &str {
        ZLIB
    }

    fn configuration_opt(
        &self,
        _name: &str,
        _options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        let configuration = ZlibCodecConfiguration::V1(ZlibCodecConfigurationV1 {
            level: ZlibCompressionLevel::try_from(self.compression.level())
                .expect("checked on init"),
        });
        Some(configuration.into())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl BytesToBytesCodecTraits for ZlibCodec {
    fn into_dyn(self: Arc<Self>) -> Arc<dyn BytesToBytesCodecTraits> {
        self as Arc<dyn BytesToBytesCodecTraits>
    }

    fn recommended_concurrency(
        &self,
        _decoded_representation: &BytesRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // zlib does not support parallel decode
        Ok(RecommendedConcurrency::new_maximum(1))
    }

    fn encode<'a>(
        &self,
        decoded_value: RawBytes<'a>,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        let mut encoder =
            flate2::read::ZlibEncoder::new(Cursor::new(decoded_value), self.compression);
        let mut out: Vec<u8> = Vec::new();
        encoder.read_to_end(&mut out)?;
        Ok(Cow::Owned(out))
    }

    fn decode<'a>(
        &self,
        encoded_value: RawBytes<'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        let mut decoder = flate2::read::ZlibDecoder::new(Cursor::new(encoded_value));
        let mut out: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(Cow::Owned(out))
    }

    fn encoded_representation(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        decoded_representation
            .size()
            .map_or(BytesRepresentation::UnboundedSize, |size| {
                // https://en.wikipedia.org/wiki/Bzip2#Implementation
                // TODO: Below assumes a maximum expansion of 1.25 for the blocks + header (4 byte) + footer (11 byte), but need to read spec
                BytesRepresentation::BoundedSize(4 + 11 + size + size.div_ceil(4))
            })
    }
}
