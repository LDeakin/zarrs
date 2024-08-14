use std::{borrow::Cow, sync::Arc};

use crate::{array::BytesRepresentation, byte_range::ByteRange};

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
        byte_ranges: &[ByteRange],
        bytes: Vec<crate::array::RawBytes<'_>>,
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
        let decoded_value_len = std::iter::zip(byte_ranges, &bytes)
            .map(|(byte_range, bytes)| match byte_range {
                ByteRange::FromStart(offset, len) => {
                    assert_eq!(bytes.len() as u64, len.unwrap_or(bytes.len() as u64));
                    *offset + bytes.len() as u64
                }
                ByteRange::FromEnd(_, len) => {
                    // TODO: this may not be correct. Not a tested code path?
                    len.unwrap_or(0).max(decoded_value.len() as u64)
                }
            })
            .max()
            .unwrap();
        decoded_value.resize(
            usize::try_from(decoded_value_len)
                .unwrap()
                .max(decoded_value.len()),
            0,
        );

        for (byte_range, bytes) in std::iter::zip(byte_ranges, bytes) {
            match byte_range {
                ByteRange::FromStart(offset, len) => {
                    assert_eq!(bytes.len() as u64, len.unwrap_or(bytes.len() as u64));
                    let offset = usize::try_from(*offset).unwrap();
                    let end = offset + bytes.len();
                    decoded_value[offset..end].copy_from_slice(&bytes);
                }
                ByteRange::FromEnd(offset, len) => {
                    assert_eq!(bytes.len() as u64, len.unwrap_or(bytes.len() as u64));
                    let offset = usize::try_from(decoded_value.len() as u64 - offset).unwrap();
                    let end = offset + bytes.len();
                    decoded_value[offset..end].copy_from_slice(&bytes);
                }
            }
        }
        let bytes_encoded = self
            .codec
            .encode(Cow::Owned(decoded_value), options)?
            .into_owned();

        self.output_handle.partial_encode(
            &[ByteRange::FromStart(0, Some(bytes_encoded.len() as u64))],
            vec![Cow::Owned(bytes_encoded)],
            options,
        )
    }
}
