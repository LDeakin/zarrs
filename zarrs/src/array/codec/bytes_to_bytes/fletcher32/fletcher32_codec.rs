use std::{borrow::Cow, sync::Arc};

use num::Integer;

use crate::{
    array::{
        codec::{
            bytes_to_bytes::strip_suffix_partial_decoder::StripSuffixPartialDecoder,
            BytesPartialDecoderTraits, BytesPartialEncoderDefault, BytesPartialEncoderTraits,
            BytesToBytesCodecTraits, CodecError, CodecOptions, CodecTraits, RecommendedConcurrency,
        },
        ArrayMetadataOptions, BytesRepresentation, RawBytes,
    },
    metadata::v3::MetadataV3,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

#[cfg(feature = "async")]
use crate::array::codec::bytes_to_bytes::strip_suffix_partial_decoder::AsyncStripSuffixPartialDecoder;

use super::{
    Fletcher32CodecConfiguration, Fletcher32CodecConfigurationV1, CHECKSUM_SIZE, IDENTIFIER,
};

/// A `fletcher32` codec implementation.
#[derive(Clone, Debug, Default)]
pub struct Fletcher32Codec;

impl Fletcher32Codec {
    /// Create a new `fletcher32` codec.
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }

    /// Create a new `fletcher32` codec.
    #[must_use]
    pub const fn new_with_configuration(_configuration: &Fletcher32CodecConfiguration) -> Self {
        Self {}
    }
}

impl CodecTraits for Fletcher32Codec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let configuration = Fletcher32CodecConfigurationV1 {};
        Some(MetadataV3::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

/// HDF5 Fletcher32.
///
/// Based on <https://github.com/Unidata/netcdf-c/blob/main/plugins/H5checksum.c#L109>.
fn h5_checksum_fletcher32(data: &[u8]) -> u32 {
    let mut len = data.len() / 2;
    let mut sum1: u32 = 0;
    let mut sum2: u32 = 0;

    // Compute checksum for pairs of bytes
    let mut data_idx = 0;
    while len > 0 {
        let tlen = len.min(360);
        len -= tlen;
        for _ in 0..tlen {
            sum1 += u32::from((u16::from(data[data_idx]) << 8u16) | u16::from(data[data_idx + 1]));
            data_idx += 2;
            sum2 += sum1;
        }
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    }

    // Check for odd # of bytes
    if len.is_odd() {
        sum1 += u32::from(u16::from(data[data_idx]) << 8);
        sum2 += sum1;
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);
    }

    // Second reduction step to reduce sums to 16 bits
    sum1 = (sum1 & 0xffff) + (sum1 >> 16);
    sum2 = (sum2 & 0xffff) + (sum2 >> 16);

    (sum2 << 16) | sum1
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl BytesToBytesCodecTraits for Fletcher32Codec {
    fn dynamic(self: Arc<Self>) -> Arc<dyn BytesToBytesCodecTraits> {
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
        let checksum = h5_checksum_fletcher32(&decoded_value).to_le_bytes();
        let mut encoded_value: Vec<u8> = Vec::with_capacity(decoded_value.len() + checksum.len());
        encoded_value.extend_from_slice(&decoded_value);
        encoded_value.extend_from_slice(&checksum);
        Ok(Cow::Owned(encoded_value))
    }

    fn decode<'a>(
        &self,
        encoded_value: RawBytes<'a>,
        _decoded_representation: &BytesRepresentation,
        options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        if encoded_value.len() >= CHECKSUM_SIZE {
            if options.validate_checksums() {
                let decoded_value = &encoded_value[..encoded_value.len() - CHECKSUM_SIZE];
                let checksum = h5_checksum_fletcher32(decoded_value).to_le_bytes();
                if checksum != encoded_value[encoded_value.len() - CHECKSUM_SIZE..] {
                    return Err(CodecError::InvalidChecksum);
                }
            }
            let decoded_value = encoded_value[..encoded_value.len() - CHECKSUM_SIZE].to_vec();
            Ok(Cow::Owned(decoded_value))
        } else {
            Err(CodecError::Other(
                "fletcher32 decoder expects a 32 bit input".to_string(),
            ))
        }
    }

    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn BytesPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(StripSuffixPartialDecoder::new(
            input_handle,
            CHECKSUM_SIZE,
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
        _decoded_representation: &BytesRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncBytesPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(AsyncStripSuffixPartialDecoder::new(
            input_handle,
            CHECKSUM_SIZE,
        )))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &BytesRepresentation,
    ) -> BytesRepresentation {
        match decoded_representation {
            BytesRepresentation::FixedSize(size) => {
                BytesRepresentation::FixedSize(size + CHECKSUM_SIZE as u64)
            }
            BytesRepresentation::BoundedSize(size) => {
                BytesRepresentation::BoundedSize(size + CHECKSUM_SIZE as u64)
            }
            BytesRepresentation::UnboundedSize => BytesRepresentation::UnboundedSize,
        }
    }
}
