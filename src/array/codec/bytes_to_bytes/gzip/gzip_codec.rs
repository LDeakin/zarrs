use std::io::{Cursor, Read};

use flate2::bufread::{GzDecoder, GzEncoder};

use crate::{
    array::{
        codec::{
            BytesPartialDecoderTraits, BytesToBytesCodecTraits, CodecError, CodecOptions,
            CodecTraits, RecommendedConcurrency,
        },
        BytesRepresentation,
    },
    metadata::Metadata,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{
    gzip_compression_level::GzipCompressionLevelError,
    gzip_configuration::GzipCodecConfigurationV1, gzip_partial_decoder, GzipCodecConfiguration,
    GzipCompressionLevel, IDENTIFIER,
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
    #[must_use]
    pub const fn new_with_configuration(configuration: &GzipCodecConfiguration) -> Self {
        let GzipCodecConfiguration::V1(configuration) = configuration;
        Self {
            compression_level: configuration.level,
        }
    }
}

impl CodecTraits for GzipCodec {
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = GzipCodecConfigurationV1 {
            level: self.compression_level,
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
impl BytesToBytesCodecTraits for GzipCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &BytesRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        Ok(RecommendedConcurrency::new_maximum(1))
    }

    fn encode(
        &self,
        decoded_value: Vec<u8>,
        _options: &CodecOptions,
    ) -> Result<Vec<u8>, CodecError> {
        let mut encoder = GzEncoder::new(
            Cursor::new(decoded_value),
            flate2::Compression::new(self.compression_level.as_u32()),
        );
        let mut out: Vec<u8> = Vec::new();
        encoder.read_to_end(&mut out)?;
        Ok(out)
    }

    fn decode(
        &self,
        encoded_value: Vec<u8>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Vec<u8>, CodecError> {
        let mut decoder = GzDecoder::new(Cursor::new(encoded_value));
        let mut out: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(out)
    }

    fn partial_decoder<'a>(
        &self,
        r: Box<dyn BytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn BytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(gzip_partial_decoder::GzipPartialDecoder::new(r)))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        r: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Box<dyn AsyncBytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            gzip_partial_decoder::AsyncGzipPartialDecoder::new(r),
        ))
    }

    fn compute_encoded_size(
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
                let blocks_overhead = BLOCK_OVERHEAD * ((size + BLOCK_SIZE - 1) / BLOCK_SIZE);
                BytesRepresentation::BoundedSize(size + HEADER_TRAILER_OVERHEAD + blocks_overhead)
            })
    }
}
