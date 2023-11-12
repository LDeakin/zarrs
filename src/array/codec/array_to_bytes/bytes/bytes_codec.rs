// Note: No validation that this codec is created *without* a specified endianness for multi-byte data types.

use crate::{
    array::{
        codec::{
            ArrayCodecTraits, ArrayPartialDecoderTraits, ArrayToBytesCodecTraits,
            BytesPartialDecoderTraits, Codec, CodecError, CodecPlugin, CodecTraits,
        },
        ArrayRepresentation, BytesRepresentation,
    },
    metadata::Metadata,
    plugin::PluginCreateError,
};

use super::{
    bytes_configuration::BytesCodecConfigurationV1, bytes_partial_decoder, reverse_endianness,
    BytesCodecConfiguration, Endianness, NATIVE_ENDIAN,
};

const IDENTIFIER: &str = "bytes";

// Register the codec.
inventory::submit! {
    CodecPlugin::new(IDENTIFIER, is_name_bytes, create_codec_bytes)
}

fn is_name_bytes(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_codec_bytes(metadata: &Metadata) -> Result<Codec, PluginCreateError> {
    let configuration: BytesCodecConfiguration = metadata.to_configuration()?;
    let codec = Box::new(BytesCodec::new_with_configuration(&configuration));
    Ok(Codec::ArrayToBytes(codec))
}

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
        decoded_representation: &ArrayRepresentation,
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

impl ArrayCodecTraits for BytesCodec {
    fn encode_opt(
        &self,
        decoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        self.do_encode_or_decode(decoded_value, decoded_representation)
    }

    fn decode_opt(
        &self,
        encoded_value: Vec<u8>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Vec<u8>, CodecError> {
        self.do_encode_or_decode(encoded_value, decoded_representation)
    }
}

impl ArrayToBytesCodecTraits for BytesCodec {
    fn partial_decoder_opt<'a>(
        &self,
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ArrayRepresentation,
        _parallel: bool,
    ) -> Result<Box<dyn ArrayPartialDecoderTraits + 'a>, CodecError> {
        Ok(Box::new(bytes_partial_decoder::BytesPartialDecoder::new(
            input_handle,
            decoded_representation.clone(),
            self.endian,
        )))
    }

    fn compute_encoded_size(
        &self,
        decoded_representation: &ArrayRepresentation,
    ) -> Result<BytesRepresentation, CodecError> {
        Ok(BytesRepresentation::FixedSize(
            decoded_representation.num_elements() * decoded_representation.element_size() as u64,
        ))
    }
}
