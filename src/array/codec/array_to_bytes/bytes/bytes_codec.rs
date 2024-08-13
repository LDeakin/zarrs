// Note: No validation that this codec is created *without* a specified endianness for multi-byte data types.

use std::sync::Arc;

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, CodecError, CodecOptions, CodecTraits,
            RecommendedConcurrency,
        },
        ArrayBytes, ArrayMetadataOptions, BytesRepresentation, ChunkRepresentation, DataTypeSize,
        RawBytes,
    },
    metadata::v3::MetadataV3,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{
    bytes_partial_decoder, reverse_endianness, BytesCodecConfiguration, BytesCodecConfigurationV1,
    Endianness, NATIVE_ENDIAN,
};

/// A `bytes` codec implementation.
#[derive(Debug, Clone)]
pub struct BytesCodec {
    endian: Option<Endianness>,
}

impl Default for BytesCodec {
    fn default() -> Self {
        Self::new(Some(NATIVE_ENDIAN))
    }
}

impl BytesCodec {
    /// Create a new `bytes` codec.
    ///
    /// `endian` is optional because an 8-bit type has no endianness.
    #[must_use]
    pub const fn new(endian: Option<Endianness>) -> Self {
        Self { endian }
    }

    /// Create a new `bytes` codec for little endian data.
    #[must_use]
    pub const fn little() -> Self {
        Self::new(Some(Endianness::Little))
    }

    /// Create a new `bytes` codec for big endian data.
    #[must_use]
    pub const fn big() -> Self {
        Self::new(Some(Endianness::Big))
    }

    /// Create a new `bytes` codec from configuration.
    #[must_use]
    pub const fn new_with_configuration(configuration: &BytesCodecConfiguration) -> Self {
        let BytesCodecConfiguration::V1(configuration) = configuration;
        Self::new(configuration.endian)
    }

    fn do_encode_or_decode<'a>(
        &self,
        mut value: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<RawBytes<'a>, CodecError> {
        match decoded_representation.data_type().size() {
            DataTypeSize::Variable => {
                return Err(CodecError::UnsupportedDataType(
                    decoded_representation.data_type().clone(),
                    super::IDENTIFIER.to_string(),
                ))
            }
            DataTypeSize::Fixed(data_type_size) => {
                let array_size = decoded_representation.num_elements() * data_type_size as u64;
                if value.len() as u64 != array_size {
                    return Err(CodecError::UnexpectedChunkDecodedSize(
                        value.len(),
                        array_size,
                    ));
                } else if data_type_size > 1 && self.endian.is_none() {
                    return Err(CodecError::Other(format!(
                        "tried to encode an array with element size {data_type_size} with endianness None"
                    )));
                }
            }
        };

        if let Some(endian) = &self.endian {
            if !endian.is_native() {
                reverse_endianness(value.to_mut(), decoded_representation.data_type());
            }
        }
        Ok(value)
    }
}

impl CodecTraits for BytesCodec {
    fn create_metadata_opt(&self, _options: &ArrayMetadataOptions) -> Option<MetadataV3> {
        let configuration = BytesCodecConfigurationV1 {
            endian: self.endian,
        };
        Some(
            MetadataV3::new_with_serializable_configuration(super::IDENTIFIER, &configuration)
                .unwrap(),
        )
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

impl ArrayCodecTraits for BytesCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        // TODO: Recomment > 1 if endianness needs changing and input is sufficiently large
        // if let Some(endian) = &self.endian {
        //     if !endian.is_native() {
        //         FIXME: Support parallel
        //         let min_elements_per_thread = 32768; // 32^3
        //         unsafe {
        //             NonZeroU64::new_unchecked(
        //                 decoded_representation.num_elements().div_ceil(min_elements_per_thread),
        //             )
        //         }
        //     }
        // }
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for BytesCodec {
    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        bytes.validate(
            decoded_representation.num_elements(),
            decoded_representation.data_type().size(),
        )?;
        let bytes = bytes.into_fixed()?;
        self.do_encode_or_decode(bytes, decoded_representation)
    }

    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        Ok(ArrayBytes::from(
            self.do_encode_or_decode(bytes, decoded_representation)?,
        ))
    }

    fn partial_decoder<'a>(
        &self,
        input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(bytes_partial_decoder::BytesPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
            self.endian,
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder<'a>(
        &'a self,
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Arc::new(
            bytes_partial_decoder::AsyncBytesPartialDecoder::new(
                input_handle,
                decoded_representation.clone(),
                self.endian,
            ),
        ))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        match decoded_representation.data_type().size() {
            DataTypeSize::Variable => {
                return Err(CodecError::UnsupportedDataType(
                    decoded_representation.data_type().clone(),
                    super::IDENTIFIER.to_string(),
                ))
            }
            DataTypeSize::Fixed(data_type_size) => Ok(BytesRepresentation::FixedSize(
                decoded_representation.num_elements() * data_type_size as u64,
            )),
        }
    }
}
