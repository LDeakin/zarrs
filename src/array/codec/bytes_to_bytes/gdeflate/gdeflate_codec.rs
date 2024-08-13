use core::mem::size_of;
use std::{borrow::Cow, sync::Arc};

use crate::{
    array::{
        codec::{
            BytesPartialDecoderTraits, BytesToBytesCodecTraits, CodecError, CodecOptions,
            CodecTraits,
        },
        ArrayMetadataOptions, BytesRepresentation, RawBytes, RecommendedConcurrency,
    },
    metadata::v3::MetadataV3,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{
    gdeflate_decode, gdeflate_partial_decoder, GDeflateCodecConfiguration,
    GDeflateCodecConfigurationV1, GDeflateCompressionLevel, GDeflateCompressionLevelError,
    GDeflateCompressor, GDEFLATE_STATIC_HEADER_LENGTH, IDENTIFIER,
};

/// A `gdeflate` codec implementation.
#[derive(Clone, Debug)]
pub struct GDeflateCodec {
    compression_level: GDeflateCompressionLevel,
}

impl GDeflateCodec {
    /// Create a new `gdeflate` codec.
    ///
    /// # Errors
    /// Returns [`GDeflateCompressionLevelError`] if `compression_level` is not valid.
    pub fn new(compression_level: u32) -> Result<Self, GDeflateCompressionLevelError> {
        let compression_level: GDeflateCompressionLevel = compression_level.try_into()?;
        // let compression_level = compression_level.into();
        Ok(Self { compression_level })
    }

    /// Create a new `gdeflate` codec from configuration.
    #[must_use]
    pub fn new_with_configuration(configuration: &GDeflateCodecConfiguration) -> Self {
        let GDeflateCodecConfiguration::V1(configuration) = configuration;
        let compression_level = configuration.level;
        Self { compression_level }
    }
}

impl CodecTraits for GDeflateCodec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let configuration = GDeflateCodecConfigurationV1 {
            level: self.compression_level,
        };
        Some(MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        true
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl BytesToBytesCodecTraits for GDeflateCodec {
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
        let compressor = GDeflateCompressor::new(self.compression_level)
            .map_err(|err| CodecError::Other(err.to_string()))?;
        let (page_sizes, encoded_bytes) = compressor
            .compress(&decoded_value)
            .map_err(|err| CodecError::Other(err.to_string()))?;
        let mut encoded_value = Vec::with_capacity(
            GDEFLATE_STATIC_HEADER_LENGTH
                + page_sizes.len() * size_of::<u64>()
                + encoded_bytes.len(),
        );

        // Header
        let decoded_value_len = u64::try_from(decoded_value.len()).unwrap();
        let num_pages = u64::try_from(page_sizes.len()).unwrap();
        encoded_value.extend_from_slice(&decoded_value_len.to_le_bytes());
        encoded_value.extend_from_slice(&num_pages.to_le_bytes());
        for page_size_compressed in page_sizes {
            let page_size_compressed = u64::try_from(page_size_compressed).unwrap();
            encoded_value.extend_from_slice(&page_size_compressed.to_le_bytes());
        }

        // Data
        encoded_value.extend_from_slice(&encoded_bytes);

        Ok(Cow::Owned(encoded_value))
    }

    fn decode<'a>(
        &self,
        encoded_value: RawBytes<'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        Ok(Cow::Owned(gdeflate_decode(&encoded_value)?))
    }

    fn partial_decoder<'a>(
        &self,
        r: Arc<dyn BytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn BytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(
            gdeflate_partial_decoder::GDeflatePartialDecoder::new(r),
        ))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        r: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncBytesPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(
            gdeflate_partial_decoder::AsyncGDeflatePartialDecoder::new(r),
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        match decoded_representation {
            BytesRepresentation::BoundedSize(size) | BytesRepresentation::FixedSize(size) => {
                let compressor = GDeflateCompressor::new(self.compression_level).unwrap(); // FIXME: Make compute_encoded_size fallible?
                let size = usize::try_from(*size).unwrap();
                let (_, compress_bound) = compressor.get_npages_compress_bound(size);
                let compress_bound = u64::try_from(compress_bound).unwrap();
                BytesRepresentation::BoundedSize(compress_bound)
            }
            BytesRepresentation::UnboundedSize => BytesRepresentation::UnboundedSize,
        }
    }
}
