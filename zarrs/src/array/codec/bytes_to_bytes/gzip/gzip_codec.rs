use std::{
    borrow::Cow,
    io::{Cursor, Read},
    sync::Arc,
};

use flate2::bufread::{GzDecoder, GzEncoder};
use zarrs_plugin::{MetadataConfiguration, PluginCreateError};

use crate::array::{
    codec::{
        BytesPartialDecoderDefault, BytesPartialDecoderTraits, BytesPartialEncoderDefault,
        BytesPartialEncoderTraits, BytesToBytesCodecTraits, CodecError, CodecMetadataOptions,
        CodecOptions, CodecTraits, RecommendedConcurrency,
    },
    BytesRepresentation, RawBytes,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderDefault;
#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{
    GzipCodecConfiguration, GzipCodecConfigurationV1, GzipCompressionLevel,
    GzipCompressionLevelError,
};

/// A `gzip` codec implementation.
#[derive(Clone, Debug)]
pub struct GzipCodec {
    compression_level: GzipCompressionLevel,
}

impl GzipCodec {
    /// Create a new `gzip` codec.
    ///
    /// # Errors
    /// Returns [`GzipCompressionLevelError`] if `compression_level` is not valid.
    pub fn new(compression_level: u32) -> Result<Self, GzipCompressionLevelError> {
        let compression_level: GzipCompressionLevel = compression_level.try_into()?;
        Ok(Self { compression_level })
    }

    /// Create a new `gzip` codec from configuration.
    ///
    /// # Errors
    /// Returns an error if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &GzipCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        match configuration {
            GzipCodecConfiguration::V1(configuration) => Ok(Self {
                compression_level: configuration.level,
            }),
            _ => Err(PluginCreateError::Other(
                "this gzip codec configuration variant is unsupported".to_string(),
            )),
        }
    }
}

impl CodecTraits for GzipCodec {
    fn identifier(&self) -> &str {
        super::IDENTIFIER
    }

    fn configuration_opt(
        &self,
        _name: &str,
        _options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        let configuration = GzipCodecConfiguration::V1(GzipCodecConfigurationV1 {
            level: self.compression_level,
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
impl BytesToBytesCodecTraits for GzipCodec {
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
        let mut encoder = GzEncoder::new(
            Cursor::new(decoded_value),
            flate2::Compression::new(self.compression_level.as_u32()),
        );
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
        let mut decoder = GzDecoder::new(Cursor::new(encoded_value));
        let mut out: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(Cow::Owned(out))
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
            self,
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
            self,
        )))
    }

    fn encoded_representation(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        decoded_representation
            .size()
            .map_or(BytesRepresentation::UnboundedSize, |size| {
                // https://www.gnu.org/software/gzip/manual/gzip.pdf
                const HEADER_TRAILER_OVERHEAD: u64 = 10 + 8; // TODO: validate that extra headers are not populated
                const BLOCK_SIZE: u64 = 32768;
                const BLOCK_OVERHEAD: u64 = 5;
                let blocks_overhead = BLOCK_OVERHEAD * size.div_ceil(BLOCK_SIZE);
                BytesRepresentation::BoundedSize(size + HEADER_TRAILER_OVERHEAD + blocks_overhead)
            })
    }
}
