// Note: No validation that this codec is created *without* a specified endianness for multi-byte data types.

use std::{borrow::Cow, sync::Arc};

use num::Integer;
use zarrs_metadata::codec::{packbits::PackBitsPaddingEncoding, PACKBITS};
use zarrs_plugin::{MetadataConfiguration, PluginCreateError};

use crate::array::{
    codec::{
        ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayPartialEncoderDefault,
        ArrayPartialEncoderTraits, ArrayToBytesCodecTraits, BytesPartialDecoderTraits,
        BytesPartialEncoderTraits, CodecError, CodecMetadataOptions, CodecOptions, CodecTraits,
        InvalidBytesLengthError, RecommendedConcurrency,
    },
    ArrayBytes, BytesRepresentation, ChunkRepresentation, RawBytes,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

#[cfg(feature = "async")]
use super::packbits_partial_decoder::AsyncPackBitsPartialDecoder;

use super::{
    element_size_bits, elements_size_bytes, packbits_partial_decoder::PackBitsPartialDecoder,
    PackBitsCodecConfiguration, PackBitsCodecConfigurationV1,
};

/// A `packbits` codec implementation.
#[derive(Debug, Clone)]
pub struct PackBitsCodec {
    padding_encoding: PackBitsPaddingEncoding,
}

impl Default for PackBitsCodec {
    fn default() -> Self {
        Self::new(PackBitsPaddingEncoding::default())
    }
}

fn padding_bits(num_elements: u64, element_size_bits: u8) -> u8 {
    let rem = ((num_elements * u64::from(element_size_bits)) % 8) as u8;
    if rem == 0 {
        0
    } else {
        8 - rem
    }
}

impl PackBitsCodec {
    /// Create a new `packbits` codec.
    #[must_use]
    pub const fn new(padding_encoding: PackBitsPaddingEncoding) -> Self {
        Self { padding_encoding }
    }

    /// Create a new `packbits` codec from configuration.
    ///
    /// # Errors
    /// Returns an error if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &PackBitsCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        match configuration {
            PackBitsCodecConfiguration::V1(configuration) => Ok(Self::new(
                configuration.padding_encoding.unwrap_or_default(),
            )),
            _ => Err(PluginCreateError::Other(
                "this bytes codec configuration variant is unsupported".to_string(),
            )),
        }
    }
}

impl CodecTraits for PackBitsCodec {
    fn identifier(&self) -> &str {
        PACKBITS
    }

    fn configuration_opt(
        &self,
        _name: &str,
        _options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        let configuration = PackBitsCodecConfiguration::V1(PackBitsCodecConfigurationV1 {
            padding_encoding: Some(self.padding_encoding),
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

impl ArrayCodecTraits for PackBitsCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToBytesCodecTraits for PackBitsCodec {
    fn into_dyn(self: Arc<Self>) -> Arc<dyn ArrayToBytesCodecTraits> {
        self as Arc<dyn ArrayToBytesCodecTraits>
    }

    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<RawBytes<'a>, CodecError> {
        // Check input length
        let bytes = bytes.into_fixed()?;
        let num_elements = decoded_representation.num_elements();
        if bytes.len() as u64 != num_elements {
            return Err(InvalidBytesLengthError::new(
                bytes.len(),
                usize::try_from(num_elements).unwrap(),
            )
            .into());
        }

        let element_size_bits = element_size_bits(decoded_representation.data_type())?;
        let elements_size_bytes =
            usize::try_from(num_elements.div_ceil(8 * u64::from(element_size_bits))).unwrap();

        // Allocate the output
        let padding_encoding_byte = match self.padding_encoding {
            PackBitsPaddingEncoding::None => 0,
            PackBitsPaddingEncoding::StartByte | PackBitsPaddingEncoding::EndByte => 1,
        };
        let mut encoded = vec![0u8; elements_size_bytes + padding_encoding_byte];

        // Set the padding encoding byte and grab the element bytes
        let padding_bits = padding_bits(num_elements, element_size_bits);
        let packed_elements = match self.padding_encoding {
            PackBitsPaddingEncoding::None => &mut encoded[..],
            PackBitsPaddingEncoding::StartByte => {
                encoded[0] = padding_bits;
                &mut encoded[1..]
            }
            PackBitsPaddingEncoding::EndByte => {
                encoded[elements_size_bytes] = padding_bits;
                &mut encoded[..elements_size_bytes]
            }
        };

        // Write the packed elements
        for (element_idx, byte) in bytes.iter().enumerate() {
            let bit_offset = element_idx * element_size_bits as usize;
            for bit in 0..element_size_bits as usize {
                let (byte_encoded, bit_encoded) = (bit_offset + bit).div_rem(&8);
                let element_bit = (byte >> bit) & 0b1;
                packed_elements[byte_encoded] |= element_bit << bit_encoded;
            }
        }

        Ok(RawBytes::from(encoded))
    }

    fn decode<'a>(
        &self,
        bytes: RawBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        let num_elements = decoded_representation.num_elements();
        let element_size_bits = element_size_bits(decoded_representation.data_type())?;
        let elements_size_bytes =
            usize::try_from(num_elements.div_ceil(8 * u64::from(element_size_bits))).unwrap();

        // Check input length
        let expected_length = elements_size_bytes
            + match self.padding_encoding {
                PackBitsPaddingEncoding::None => 0,
                PackBitsPaddingEncoding::StartByte | PackBitsPaddingEncoding::EndByte => 1,
            };
        if bytes.len() != expected_length {
            return Err(InvalidBytesLengthError::new(bytes.len(), expected_length).into());
        }

        let padding_bits = padding_bits(num_elements, element_size_bits);
        let packed_elements = match self.padding_encoding {
            PackBitsPaddingEncoding::None => &bytes[..],
            PackBitsPaddingEncoding::StartByte => {
                if bytes[0] != padding_bits {
                    return Err(CodecError::Other(
                        "the packbits padding encoding start byte is incorrect".to_string(),
                    ));
                }
                &bytes[1..]
            }
            PackBitsPaddingEncoding::EndByte => {
                if bytes[elements_size_bytes] != padding_bits {
                    return Err(CodecError::Other(
                        "the packbits padding encoding end byte is incorrect".to_string(),
                    ));
                }
                &bytes[..elements_size_bytes]
            }
        };

        // Allocate the output
        let num_elements = usize::try_from(num_elements).unwrap();
        let mut elements = vec![0u8; num_elements];

        for (element_idx, element) in elements.iter_mut().enumerate() {
            let bit_offset = element_idx * element_size_bits as usize;
            for bit in 0..element_size_bits as usize {
                let (byte_encoded, bit_encoded) = (bit_offset + bit).div_rem(&8);
                let element_bit = (packed_elements[byte_encoded] >> bit_encoded) & 0b1;
                *element |= element_bit << bit;
            }
        }

        Ok(ArrayBytes::Fixed(Cow::Owned(elements)))
    }

    fn partial_decoder(
        self: Arc<Self>,
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<Arc<dyn ArrayPartialDecoderTraits>, CodecError> {
        Ok(Arc::new(PackBitsPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
            self.padding_encoding,
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
        Ok(Arc::new(AsyncPackBitsPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
            self.padding_encoding,
        )))
    }

    fn encoded_representation(
        &self,
        decoded_representation: &ChunkRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        let elements_size_bytes = elements_size_bytes(
            decoded_representation.data_type(),
            decoded_representation.num_elements(),
        )?;
        let padding_encoding_byte = match self.padding_encoding {
            PackBitsPaddingEncoding::None => 0,
            PackBitsPaddingEncoding::StartByte | PackBitsPaddingEncoding::EndByte => 1,
        };
        Ok(BytesRepresentation::FixedSize(
            elements_size_bytes + padding_encoding_byte,
        ))
    }
}
