use std::{borrow::Cow, sync::Arc};

use zarrs_metadata::codec::ZSTD;
use zarrs_plugin::{MetadataConfiguration, PluginCreateError};
use zstd::zstd_safe;

use crate::array::{
    codec::{
        BytesToBytesCodecTraits, CodecError, CodecMetadataOptions, CodecOptions, CodecTraits,
        RecommendedConcurrency,
    },
    BytesRepresentation, RawBytes,
};

use super::{ZstdCodecConfiguration, ZstdCodecConfigurationV1};

/// A `zstd` codec implementation.
#[derive(Clone, Debug)]
pub struct ZstdCodec {
    compression: zstd_safe::CompressionLevel,
    checksum: bool,
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
    ///
    /// # Errors
    /// Returns an error if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &ZstdCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        let (compression, checksum) = match configuration {
            ZstdCodecConfiguration::V1(configuration) => {
                (configuration.level, configuration.checksum)
            }
            ZstdCodecConfiguration::Numcodecs(configuration) => (configuration.level, false),
            _ => Err(PluginCreateError::Other(
                "this zstd codec configuration variant is unsupported".to_string(),
            ))?,
        };
        Ok(Self {
            compression: compression.into(),
            checksum,
        })
    }
}

impl CodecTraits for ZstdCodec {
    fn identifier(&self) -> &str {
        ZSTD
    }

    fn configuration_opt(
        &self,
        _name: &str,
        _options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        let configuration = ZstdCodecConfiguration::V1(ZstdCodecConfigurationV1 {
            level: self.compression.into(),
            checksum: self.checksum,
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
impl BytesToBytesCodecTraits for ZstdCodec {
    fn into_dyn(self: Arc<Self>) -> Arc<dyn BytesToBytesCodecTraits> {
        self as Arc<dyn BytesToBytesCodecTraits>
    }

    fn recommended_concurrency(
        &self,
        _decoded_representation: &BytesRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // TODO: zstd supports multithread, but at what point is it good to kick in?
        Ok(RecommendedConcurrency::new_maximum(1))
    }

    fn encode<'a>(
        &self,
        decoded_value: RawBytes<'a>,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        let mut result = Vec::<u8>::new();
        let mut encoder = zstd::Encoder::new(&mut result, self.compression)?;
        encoder.include_checksum(self.checksum)?;
        // if parallel {
        //     let n_threads = std::thread::available_parallelism().unwrap().get();
        //     encoder.multithread(u32::try_from(n_threads).unwrap())?; // TODO: Check overhead of zstd par_encode
        // }
        std::io::copy(&mut std::io::Cursor::new(&decoded_value), &mut encoder)?;
        encoder.finish()?;
        Ok(Cow::Owned(result))
    }

    fn decode<'a>(
        &self,
        encoded_value: RawBytes<'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        zstd::decode_all(std::io::Cursor::new(&encoded_value))
            .map_err(CodecError::IOError)
            .map(Cow::Owned)
    }

    fn encoded_representation(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        decoded_representation
            .size()
            .map_or(BytesRepresentation::UnboundedSize, |size| {
                // https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md
                // TODO: Validate the window/block relationship
                const HEADER_TRAILER_OVERHEAD: u64 = 4 + 14 + 4;
                const MIN_WINDOW_SIZE: u64 = 1000; // 1KB
                const BLOCK_OVERHEAD: u64 = 3;
                let blocks_overhead = BLOCK_OVERHEAD * size.div_ceil(MIN_WINDOW_SIZE);
                BytesRepresentation::BoundedSize(size + HEADER_TRAILER_OVERHEAD + blocks_overhead)
            })
    }
}
