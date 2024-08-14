use zarrs_storage::{byte_range::ByteRange, StoreKey, StoreKeyStartValue, WritableStorage};

use crate::array::RawBytes;

use super::{BytesPartialEncoderTraits, CodecError, CodecOptions};

/// A [`WritableStorage`] store value partial encoder.
pub struct StoragePartialEncoder {
    storage: WritableStorage,
    key: StoreKey,
}

impl StoragePartialEncoder {
    /// Create a new storage partial encoder.
    pub fn new(storage: WritableStorage, key: StoreKey) -> Self {
        Self { storage, key }
    }
}

impl BytesPartialEncoderTraits for StoragePartialEncoder {
    fn erase(&self) -> Result<(), CodecError> {
        Ok(self.storage.erase(&self.key)?)
    }

    fn partial_encode(
        &self,
        byte_ranges: &[ByteRange],
        bytes: Vec<RawBytes<'_>>,
        _options: &CodecOptions,
    ) -> Result<(), CodecError> {
        if byte_ranges.len() != bytes.len() {
            return Err(CodecError::Other(
                "byte_ranges and bytes have a length mismatch".to_string(),
            ));
        }

        let mut key_start_values = Vec::with_capacity(bytes.len());
        for (byte_range, bytes) in std::iter::zip(byte_ranges, &bytes) {
            let byte_range_start = match byte_range {
                ByteRange::FromEnd(_, _) => Err(CodecError::Other("BytesPartialEncoderTraits::partial_encode does not support from end byte ranges".to_string())),
                ByteRange::FromStart(start, None) => {
                        Ok(*start)
                },
                ByteRange::FromStart(start, Some(length)) => {
                    if bytes.len() as u64 == *length {
                        Ok(*start)
                    } else {
                        Err(CodecError::Other("BytesPartialEncoderTraits::partial_encode incompatible byte range and bytes length".to_string()))
                    }
                },
            }?;
            key_start_values.push(StoreKeyStartValue::new(
                self.key.clone(),
                byte_range_start,
                bytes,
            ));
        }

        Ok(self.storage.set_partial_values(&key_start_values)?)
    }
}
