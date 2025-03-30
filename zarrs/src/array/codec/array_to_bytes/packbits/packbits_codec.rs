#![allow(clippy::similar_names)]

use std::{borrow::Cow, sync::Arc};

use num::Integer;
use zarrs_metadata::codec::{packbits::PackBitsPaddingEncoding, PACKBITS};
use zarrs_plugin::{MetadataConfiguration, PluginCreateError};

use crate::array::{
    codec::{
        array_to_bytes::packbits::div_rem_8bit, ArrayCodecTraits, ArrayPartialDecoderTraits,
        ArrayToBytesCodecTraits, BytesPartialDecoderTraits, CodecError, CodecMetadataOptions,
        CodecOptions, CodecTraits, InvalidBytesLengthError, RecommendedConcurrency,
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
        let data_type_size_dec =
            decoded_representation
                .data_type()
                .fixed_size()
                .ok_or_else(|| {
                    CodecError::Other(
                        "data type must have a fixed size for packbits codec".to_string(),
                    )
                })?;
        let num_elements = decoded_representation.num_elements();
        if bytes.len() as u64 != num_elements * data_type_size_dec as u64 {
            return Err(InvalidBytesLengthError::new(
                bytes.len(),
                usize::try_from(num_elements * data_type_size_dec as u64).unwrap(),
            )
            .into());
        }

        let element_size_bits = element_size_bits(decoded_representation.data_type())?;
        let elements_size_bytes =
            usize::try_from((num_elements * u64::from(element_size_bits)).div_ceil(8)).unwrap();

        // Allocate the output
        let padding_encoding_byte = match self.padding_encoding {
            PackBitsPaddingEncoding::None => 0,
            PackBitsPaddingEncoding::StartByte | PackBitsPaddingEncoding::EndByte => 1,
        };
        let mut bytes_enc = vec![0u8; elements_size_bytes + padding_encoding_byte];

        // Set the padding encoding byte and grab the element bytes
        let padding_bits = padding_bits(num_elements, element_size_bits);
        let packed_elements = match self.padding_encoding {
            PackBitsPaddingEncoding::None => &mut bytes_enc[..],
            PackBitsPaddingEncoding::StartByte => {
                bytes_enc[0] = padding_bits;
                &mut bytes_enc[1..]
            }
            PackBitsPaddingEncoding::EndByte => {
                bytes_enc[elements_size_bytes] = padding_bits;
                &mut bytes_enc[..elements_size_bytes]
            }
        };

        // Encode the elements
        let num_elements = usize::try_from(num_elements).unwrap();
        let element_size_bits = usize::from(element_size_bits);
        for element_idx in 0..num_elements {
            let bit_offset = element_idx * element_size_bits;
            for bit in bit_offset..bit_offset + element_size_bits {
                let (byte_enc, bit_enc) = bit.div_rem(&8);
                let (byte_dec, bit_dec) = div_rem_8bit(bit, element_size_bits);
                packed_elements[byte_enc] |= ((bytes[byte_dec] >> (bit_dec % 8)) & 0b1) << bit_enc;
            }
        }

        Ok(RawBytes::from(bytes_enc))
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
            usize::try_from((num_elements * u64::from(element_size_bits)).div_ceil(8)).unwrap();
        let data_type_size_dec =
            decoded_representation
                .data_type()
                .fixed_size()
                .ok_or_else(|| {
                    CodecError::Other(
                        "data type must have a fixed size for packbits codec".to_string(),
                    )
                })?;

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
        let mut bytes_dec = vec![0u8; num_elements * data_type_size_dec];

        // Decode the elements
        let element_size_bits = usize::from(element_size_bits);
        for element_idx in 0..num_elements {
            let bit_offset = element_idx * element_size_bits;
            for bit in bit_offset..bit_offset + element_size_bits {
                let (byte_enc, bit_enc) = bit.div_rem(&8);
                let (byte_dec, bit_dec) = div_rem_8bit(bit, element_size_bits);
                bytes_dec[byte_dec] |= ((packed_elements[byte_enc] >> bit_enc) & 0b1) << bit_dec;
            }
        }

        Ok(ArrayBytes::Fixed(Cow::Owned(bytes_dec)))
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
