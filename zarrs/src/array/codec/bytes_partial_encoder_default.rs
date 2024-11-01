use std::{borrow::Cow, sync::Arc};

use zarrs_storage::byte_range::ByteOffset;

use crate::array::BytesRepresentation;

use super::{BytesPartialDecoderTraits, BytesPartialEncoderTraits, BytesToBytesCodecTraits};

/// The default array (chunk) partial encoder. Decodes the entire chunk, updates it, and writes the entire chunk.
pub struct BytesPartialEncoderDefault {
    input_handle: Arc<dyn BytesPartialDecoderTraits>,
    output_handle: Arc<dyn BytesPartialEncoderTraits>,
    decoded_representation: BytesRepresentation,
    codec: Arc<dyn BytesToBytesCodecTraits>,
}

impl BytesPartialEncoderDefault {
    /// Create a new [`BytesPartialEncoderDefault`].
    #[must_use]
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits>,
        output_handle: Arc<dyn BytesPartialEncoderTraits>,
        decoded_representation: BytesRepresentation,
        codec: Arc<dyn BytesToBytesCodecTraits>,
    ) -> Self {
        Self {
            input_handle,
            output_handle,
            decoded_representation,
            codec,
        }
    }
}

impl BytesPartialEncoderTraits for BytesPartialEncoderDefault {
    fn erase(&self) -> Result<(), super::CodecError> {
        self.output_handle.erase()
    }

    fn partial_encode(
        &self,
        offsets_and_bytes: &[(ByteOffset, crate::array::RawBytes<'_>)],
        options: &super::CodecOptions,
    ) -> Result<(), super::CodecError> {
        let encoded_value = self.input_handle.decode(options)?.map(Cow::into_owned);

        let mut decoded_value = if let Some(encoded_value) = encoded_value {
            self.codec
                .decode(
                    Cow::Owned(encoded_value),
                    &self.decoded_representation,
                    options,
                )?
                .into_owned()
        } else {
            vec![]
        };

        // The decoded value must be resized to the maximum byte range end
        let decoded_value_len = offsets_and_bytes
            .iter()
            .map(|(offset, bytes)| usize::try_from(offset + bytes.len() as u64).unwrap())
            .max()
            .unwrap();
        decoded_value.resize(decoded_value_len, 0);

        for (offset, bytes) in offsets_and_bytes {
            let start = usize::try_from(*offset).unwrap();
            decoded_value[start..start + bytes.len()].copy_from_slice(bytes);
        }

        let bytes_encoded = self
            .codec
            .encode(Cow::Owned(decoded_value), options)?
            .into_owned();

        self.output_handle
            .partial_encode(&[(0, Cow::Owned(bytes_encoded))], options)
    }
}
