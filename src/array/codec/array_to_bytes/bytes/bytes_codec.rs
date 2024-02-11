// Note: No validation that this codec is created *without* a specified endianness for multi-byte data types.

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, CodecError, CodecTraits, DecodeOptions, EncodeOptions,
            PartialDecoderOptions, RecommendedConcurrency,
        },
        BytesRepresentation, ChunkRepresentation,
    },
    metadata::Metadata,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{
    bytes_configuration::BytesCodecConfigurationV1, bytes_partial_decoder, reverse_endianness,
    BytesCodecConfiguration, Endianness, IDENTIFIER, NATIVE_ENDIAN,
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

    fn do_encode_or_decode(
        &self,
        mut value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<Vec<u8>, CodecError> {
        if value.len() as u64 != decoded_representation.size() {
            return Err(CodecError::UnexpectedChunkDecodedSize(
                value.len(),
                decoded_representation.size(),
            ));
        } else if decoded_representation.element_size() > 1 && self.endian.is_none() {
            return Err(CodecError::Other(format!(
                "tried to encode an array with element size {} with endianness None",
                decoded_representation.size()
            )));
        }

        if let Some(endian) = &self.endian {
            if !endian.is_native() {
                reverse_endianness(&mut value, decoded_representation.data_type());
            }
        }
        Ok(value)
    }
}

impl CodecTraits for BytesCodec {
    fn create_metadata(&self) -> Option<Metadata> {
        let configuration = BytesCodecConfigurationV1 {
            endian: self.endian,
        };
        Some(Metadata::new_with_serializable_configuration(IDENTIFIER, &configuration).unwrap())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
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
        //                 (decoded_representation.num_elements() + min_elements_per_thread - 1)
        //                     / min_elements_per_thread,
        //             )
        //         }
        //     }
        // }
        Ok(RecommendedConcurrency::one())
    }

    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        _options: &EncodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        self.do_encode_or_decode(decoded_value, decoded_representation)
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ChunkRepresentation,
        _options: &DecodeOptions,
    ) -> Result<Vec<u8>, CodecError> {
        self.do_encode_or_decode(encoded_value, decoded_representation)
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for BytesCodec {
    fn partial_decoder_opt<'a>(
        &self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &PartialDecoderOptions,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(bytes_partial_decoder::BytesPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
            self.endian,
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder_opt<'a>(
        &'a self,
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &PartialDecoderOptions,
    ) -> Result<Box<dyn AsyncArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(
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
        Ok(BytesRepresentation::FixedSize(
            decoded_representation.num_elements() * decoded_representation.element_size() as u64,
        ))
    }
}
