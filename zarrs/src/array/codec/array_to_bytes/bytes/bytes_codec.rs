// Note: No validation that this codec is created *without* a specified endianness for multi-byte data types.

use std::sync::Arc;

use zarrs_data_type::{DataType, DataTypeExtensionError};
use zarrs_plugin::MetadataConfiguration;

use crate::array::{
    codec::{
        ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayPartialEncoderDefault,
        ArrayPartialEncoderTraits, ArrayToBytesCodecTraits, BytesPartialDecoderTraits,
        BytesPartialEncoderTraits, CodecError, CodecMetadataOptions, CodecOptions, CodecTraits,
        InvalidBytesLengthError, RecommendedConcurrency,
    },
    ArrayBytes, BytesRepresentation, ChunkRepresentation, DataTypeSize, RawBytes,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{
    bytes_partial_decoder, reverse_endianness, BytesCodecConfiguration, BytesCodecConfigurationV1,
    Endianness,
};

/// A `bytes` codec implementation.
#[derive(Debug, Clone)]
pub struct BytesCodec {
    endian: Option<Endianness>,
}

impl Default for BytesCodec {
    fn default() -> Self {
        Self::new(Some(Endianness::native()))
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
                ));
            }
            DataTypeSize::Fixed(data_type_size) => {
                let array_size =
                    usize::try_from(decoded_representation.num_elements() * data_type_size as u64)
                        .unwrap();
                if value.len() != array_size {
                    return Err(InvalidBytesLengthError::new(value.len(), array_size).into());
                } else if data_type_size > 1 && self.endian.is_none() {
                    return Err(CodecError::Other(format!(
                        "tried to encode an array with element size {data_type_size} with endianness None"
                    )));
                }
            }
        }

        if let Some(endian) = &self.endian {
            if !endian.is_native() {
                reverse_endianness(value.to_mut(), decoded_representation.data_type());
            }
        }
        Ok(value)
    }
}

impl CodecTraits for BytesCodec {
    fn identifier(&self) -> &str {
        super::IDENTIFIER
    }

    fn configuration_opt(
        &self,
        _name: &str,
        _options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        let configuration = BytesCodecConfiguration::V1(BytesCodecConfigurationV1 {
            endian: self.endian,
        });
        Some(configuration.into())
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
    fn dynamic(self: Arc<Self>) -> Arc<dyn ArrayToBytesCodecTraits> {
        self as Arc<dyn ArrayToBytesCodecTraits>
    }

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
        let bytes_encoded = match decoded_representation.data_type() {
            DataType::Extension(ext) => ext
                .codec_bytes()?
                .encode(bytes, self.endian)
                .map_err(DataTypeExtensionError::from)?,
            _ => self.do_encode_or_decode(bytes, decoded_representation)?,
        };
        Ok(bytes_encoded)
    }

    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        let bytes_decoded = match decoded_representation.data_type() {
            DataType::Extension(ext) => ext
                .codec_bytes()?
                .decode(bytes, self.endian)
                .map_err(DataTypeExtensionError::from)?,
            _ => self.do_encode_or_decode(bytes, decoded_representation)?,
        };
        Ok(ArrayBytes::from(bytes_decoded))
    }

    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(bytes_partial_decoder::BytesPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
            self.endian,
        )))
    }

    fn partial_encoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialEncoderTraits>, CodecError> {
        Ok(Arc::new(ArrayPartialEncoderDefault::new(
            input_handle,
            output_handle,
            decoded_representation.clone(),
            self,
        )))
    }

    #[cfg(feature = "async")]
    async fn async_partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn AsyncArrayPartialDecoderTraits>, CodecError> {
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
            DataTypeSize::Variable => Err(CodecError::UnsupportedDataType(
                decoded_representation.data_type().clone(),
                super::IDENTIFIER.to_string(),
            )),
            DataTypeSize::Fixed(data_type_size) => Ok(BytesRepresentation::FixedSize(
                decoded_representation.num_elements() * data_type_size as u64,
            )),
        }
    }
}
