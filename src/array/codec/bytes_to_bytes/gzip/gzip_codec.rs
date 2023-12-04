use std::io::{Cursor, Read};

use flate2::bufread::{GzDecoder, GzEncoder};

use crate::{
    array::{
        codec::{
            BytesPartialDecoderTraits, BytesToBytesCodecTraits, Codec, CodecError, CodecPlugin,
            CodecTraits,
        },
        BytesRepresentation,
    },
    metadata::Metadata,
    plugin::PluginCreateError,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{
    gzip_compression_level::GzipCompressionLevelError,
    gzip_configuration::GzipCodecConfigurationV1, gzip_partial_decoder, GzipCodecConfiguration,
    GzipCompressionLevel,
};

const IDENTIFIER: &str = "gzip";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_gzip, create_codec_gzip)
}

fn is_name_gzip(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_gzip(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: GzipCodecConfiguration = metadata.to_configuration()?;
    let codec = Box::new(GzipCodec::new_with_configuration(&configuration));
    Ok(Codec::BytesToBytes(codec))
}

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
    fn encode_opt(&self, decoded_value: Vec<u8>, _parallel: bool) -> Result<Vec<u8>, CodecError> {
        let mut encoder = GzEncoder::new(
            Cursor::new(decoded_value),
            flate2::Compression::new(self.compression_level.as_u32()),
        );
        let mut out: Vec<u8> = Vec::new();
        encoder.read_to_end(&mut out)?;
        Ok(out)
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        let mut decoder = GzDecoder::new(Cursor::new(encoded_value));
        let mut out: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(out)
    }

    #[cfg(feature = "async")]
    async fn async_encode_opt(
        &self,
        decoded_value: Vec<u8>,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        self.encode_opt(decoded_value, parallel)
    }

    #[cfg(feature = "async")]
    async fn async_decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &BytesRepresentation,
        parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        // FIXME: Remove
        self.decode_opt(encoded_value, decoded_representation, parallel)
    }

    fn partial_decoder_opt<'a>(
        &self,
        r: Box<dyn BytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn BytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(gzip_partial_decoder::GzipPartialDecoder::new(r)))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        r: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
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
