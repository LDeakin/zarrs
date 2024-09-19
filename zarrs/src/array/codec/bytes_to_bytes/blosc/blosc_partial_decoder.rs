use std::{borrow::Cow, sync::Arc};

use crate::{
    array::{
        codec::{
            bytes_to_bytes::blosc::blosc_nbytes, BytesPartialDecoderTraits, CodecError,
            CodecOptions,
        },
        RawBytes,
    },
    byte_range::ByteRange,
};

#[cfg(feature = "async")]
use crate::array::codec::AsyncBytesPartialDecoderTraits;

use super::{blosc_decompress_bytes_partial, blosc_typesize, blosc_validate};

/// Partial decoder for the `blosc` codec.
pub struct BloscPartialDecoder<'a> {
    input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
}

impl<'a> BloscPartialDecoder<'a> {
    pub fn new(input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self { input_handle }
    }
}

impl BytesPartialDecoderTraits for BloscPartialDecoder<'_> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options)?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        if let Some(_destsize) = blosc_validate(&encoded_value) {
            let nbytes = blosc_nbytes(&encoded_value);
            let typesize = blosc_typesize(&encoded_value);
            if let (Some(nbytes), Some(typesize)) = (nbytes, typesize) {
                let mut decoded_byte_ranges = Vec::with_capacity(decoded_regions.len());
                for byte_range in decoded_regions {
                    let start = usize::try_from(byte_range.start(nbytes as u64)).unwrap();
                    let end = usize::try_from(byte_range.end(nbytes as u64)).unwrap();
                    decoded_byte_ranges.push(
                        blosc_decompress_bytes_partial(
                            &encoded_value,
                            start,
                            end - start,
                            typesize,
                        )
                        .map(Cow::Owned)
                        .map_err(|err| CodecError::from(err.to_string()))?,
                    );
                }
                return Ok(Some(decoded_byte_ranges));
            }
        }
        Err(CodecError::from("blosc encoded value is invalid"))
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `blosc` codec.
pub struct AsyncBloscPartialDecoder {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>,
}

#[cfg(feature = "async")]
impl AsyncBloscPartialDecoder {
    pub fn new(input_handle: Arc<dyn AsyncBytesPartialDecoderTraits>) -> Self {
        Self { input_handle }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for AsyncBloscPartialDecoder {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        let encoded_value = self.input_handle.decode(options).await?;
        let Some(encoded_value) = encoded_value else {
            return Ok(None);
        };

        if let Some(_destsize) = blosc_validate(&encoded_value) {
            let nbytes = blosc_nbytes(&encoded_value);
            let typesize = blosc_typesize(&encoded_value);
            if let (Some(nbytes), Some(typesize)) = (nbytes, typesize) {
                let mut decoded_byte_ranges = Vec::with_capacity(decoded_regions.len());
                for byte_range in decoded_regions {
                    let start = usize::try_from(byte_range.start(nbytes as u64)).unwrap();
                    let end = usize::try_from(byte_range.end(nbytes as u64)).unwrap();
                    decoded_byte_ranges.push(
                        blosc_decompress_bytes_partial(
                            &encoded_value,
                            start,
                            end - start,
                            typesize,
                        )
                        .map(Cow::Owned)
                        .map_err(|err| CodecError::from(err.to_string()))?,
                    );
                }
                return Ok(Some(decoded_byte_ranges));
            }
        }
        Err(CodecError::from("blosc encoded value is invalid"))
    }
}
