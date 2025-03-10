use std::{borrow::Cow, sync::Arc};

use zarrs_storage::byte_range::{extract_byte_ranges, ByteRange};

use crate::array::{BytesRepresentation, RawBytes};

use super::{AsyncBytesPartialDecoderTraits, BytesToBytesCodecTraits, CodecError, CodecOptions};

/// The default asynchronous array (chunk) partial decoder. Decodes the entire chunk and extracts the desired region.
pub struct AsyncBytesPartialDecoderDefault {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
    decoded_representation: BytesRepresentation,
    codec: Arc<dyn BytesToBytesCodecTraits>,
}

impl AsyncBytesPartialDecoderDefault {
    /// Create a new [`AsyncBytesPartialDecoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
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

#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncBytesPartialDecoderDefault {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options).await?;
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
