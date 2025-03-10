use std::{borrow::Cow, sync::Arc};

use zarrs_storage::byte_range::{extract_byte_ranges, ByteRange};

use crate::array::{BytesRepresentation, RawBytes};

use super::{BytesPartialDecoderTraits, BytesToBytesCodecTraits, CodecError, CodecOptions};

/// The default array (chunk) partial decoder. Decodes the entire chunk and extracts the desired region.
pub struct BytesPartialDecoderDefault {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
    decoded_representation: BytesRepresentation,
    codec: Arc<dyn BytesToBytesCodecTraits>,
}

impl BytesPartialDecoderDefault {
    /// Create a new [`BytesPartialDecoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        decoded_representation: BytesRepresentation,
        codec: Arc<dyn BytesToBytesCodecTraits>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            codec,
        }
    }
}

impl BytesPartialDecoderTraits for BytesPartialDecoderDefault {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options)?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        let decoded_value = self
            .codec
            .decode(encoded_value, &self.decoded_representation, options)?
            .into_owned();

        Ok(Some(
            extract_byte_ranges(&decoded_value, decoded_regions)
                .map_err(CodecError::InvalidByteRangeError)?
                .into_iter()
                .map(Cow::Owned)
                .collect(),
        ))
    }
}
