use zstd::zstd_safe;

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

use super::{zstd_partial_decoder, ZstdCodecConfiguration, ZstdCodecConfigurationV1};

const IDENTIFIER: &str = "zstd";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_zstd, create_codec_zstd)
}

fn is_name_zstd(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_zstd(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: ZstdCodecConfiguration = metadata.to_configuration()?;
    let codec = Box::new(ZstdCodec::new_with_configuration(&configuration));
    Ok(Codec::BytesToBytes(codec))
}

/// A Zstd codec implementation.
#[derive(Clone, Debug)]
pub struct ZstdCodec {
    compression: zstd_safe::CompressionLevel,
    checksum: bool, // FIXME: Not using checksum
}

impl ZstdCodec {
    /// Create a new `Zstd` codec.
    #[must_use]
    pub const fn new(compression: zstd_safe::CompressionLevel, checksum: bool) -> Self {
        Self {
            compression,
            checksum,
        }
    }

    /// Create a new `Zstd` codec from configuration.
    #[must_use]
    pub fn new_with_configuration(configuration: &ZstdCodecConfiguration) -> Self {
        let ZstdCodecConfiguration::V1(configuration) = configuration;
        Self {
            compression: configuration.level.clone().into(),
            checksum: configuration.checksum,
        }
    }
}

impl CodecTraits for ZstdCodec {
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = ZstdCodecConfigurationV1 {
            level: self.compression.into(),
            checksum: self.checksum,
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

impl BytesToBytesCodecTraits for ZstdCodec {
    fn encode(&self, decoded_value: Vec<u8>) -> Result<Vec<u8>, CodecError> {
        zstd::encode_all(decoded_value.as_slice(), self.compression).map_err(CodecError::IOError)
    }

    fn decode(
        &self,
        encoded_value: Vec<u8>,
        _decoded_representation: &BytesRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        zstd::decode_all(encoded_value.as_slice()).map_err(CodecError::IOError)
    }

    fn partial_decoder<'a>(
        &self,
        r: Box<dyn BytesPartialDecoderTraits + 'a>,
    ) -> Box<dyn BytesPartialDecoderTraits + 'a> {
        Box::new(zstd_partial_decoder::ZstdPartialDecoder::new(r))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        match decoded_representation.size() {
            Some(size) => {
                // https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md
                // TODO: Validate the window/block relationship
                const HEADER_TRAILER_OVERHEAD: u64 = 4 + 14 + 4;
                const MIN_WINDOW_SIZE: u64 = 1000; // 1KB
                const BLOCK_OVERHEAD: u64 = 3;
                let blocks_overhead =
                    BLOCK_OVERHEAD * ((size + MIN_WINDOW_SIZE - 1) / MIN_WINDOW_SIZE);
                BytesRepresentation::BoundedSize(size + HEADER_TRAILER_OVERHEAD + blocks_overhead)
            }
            None => BytesRepresentation::UnboundedSize,
        }
    }
}
