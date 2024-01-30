use crate::{
    array::{
        codec::{
            BytesPartialDecoderTraits, BytesToBytesCodecTraits, Codec, CodecError, CodecPlugin,
            CodecTraits,
        },
        BytesRepresentation,
    },
    metadata::Metadata,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{
    lzma_configuration::{LzmaCodecConfiguration, LzmaCodecConfigurationV1},
    lzma_partial_decoder, LzmaCompressionLevel,
};

const IDENTIFIER: &str = "lzma";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_lzma, create_codec_lzma)
}

fn is_name_lzma(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_lzma(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: LzmaCodecConfiguration = metadata
        .to_configuration()
        .map_err(|_| PluginMetadataInvalidError::new(IDENTIFIER, "codec", metadata.clone()))?;
    let codec = Box::new(LzmaCodec::new_with_configuration(&configuration));
    Ok(Codec::BytesToBytes(codec))
}

/// A `lzma` codec implementation.
#[derive(Clone, Debug)]
pub struct LzmaCodec {
    compression_level: u32,
}

impl LzmaCodec {
    /// Create a new `lzma` codec.
    pub fn new(compression_level: LzmaCompressionLevel) -> Self {
        Self {
            compression_level: compression_level.as_u32(),
        }
    }

    /// Create a new `lzma` codec from configuration.
    ///
    /// # Errors
    /// Returns [`PluginCreateError`] if the configuration is not supported.
    pub fn new_with_configuration(configuration: &LzmaCodecConfiguration) -> Self {
        let LzmaCodecConfiguration::V1(configuration) = configuration;
        Self::new(configuration.level)
    }
}

impl CodecTraits for LzmaCodec {
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = LzmaCodecConfigurationV1 {
            level: LzmaCompressionLevel(self.compression_level),
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
impl BytesToBytesCodecTraits for LzmaCodec {
    fn encode_opt(&self, decoded_value: Vec<u8>, _parallel: bool) -> Result<Vec<u8>, CodecError> {
        lzma::compress(&decoded_value, self.compression_level)
            .map_err(|err| CodecError::Other(err.to_string()))
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        lzma::decompress(&encoded_value).map_err(|err| CodecError::Other(err.to_string()))
    }

    fn partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn BytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(lzma_partial_decoder::LzmaPartialDecoder::new(
            input_handle,
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn AsyncBytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
            lzma_partial_decoder::AsyncLzmaPartialDecoder::new(input_handle),
        ))
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
