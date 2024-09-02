//! A cache for partial decoders.

use std::{borrow::Cow, marker::PhantomData};

use crate::{
    array::RawBytes,
    byte_range::{extract_byte_ranges, ByteRange},
};

use super::{BytesPartialDecoderTraits, CodecError, CodecOptions};

#[cfg(feature = "async")]
use super::AsyncBytesPartialDecoderTraits;

/// A cache for a [`BytesPartialDecoderTraits`] partial decoder.
pub struct BytesPartialDecoderCache<'a> {
    cache: Option<Vec<u8>>,
    phantom: PhantomData<&'a ()>,
}

impl<'a> BytesPartialDecoderCache<'a> {
    /// Create a new partial decoder cache.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if caching fails.
    pub fn new(
        input_handle: &dyn BytesPartialDecoderTraits,
        options: &CodecOptions,
    ) -> Result<Self, CodecError> {
        let cache = input_handle
            .partial_decode(&[ByteRange::FromStart(0, None)], options)?
            .map(|mut bytes| bytes.remove(0).into_owned());
        Ok(Self {
            cache,
            phantom: PhantomData,
        })
    }

    #[cfg(feature = "async")]
    /// Create a new asynchronous partial decoder cache.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if caching fails.
    pub async fn async_new(
        input_handle: &dyn AsyncBytesPartialDecoderTraits,
        options: &CodecOptions,
    ) -> Result<BytesPartialDecoderCache<'a>, CodecError> {
        let cache = input_handle
            .partial_decode(&[ByteRange::FromStart(0, None)], options)
            .await?
            .map(|mut bytes| bytes.remove(0).into_owned());
        Ok(Self {
            cache,
            phantom: PhantomData,
        })
    }
}

impl BytesPartialDecoderTraits for BytesPartialDecoderCache<'_> {
    fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        _options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        Ok(match &self.cache {
            Some(bytes) => Some(
                extract_byte_ranges(bytes, decoded_regions)
                    .map_err(CodecError::InvalidByteRangeError)?
                    .into_iter()
                    .map(Cow::Owned)
                    .collect(),
            ),
            None => None,
        })
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for BytesPartialDecoderCache<'_> {
    async fn partial_decode(
        &self,
        decoded_regions: &[ByteRange],
        options: &CodecOptions,
    ) -> Result<Option<Vec<RawBytes<'_>>>, CodecError> {
        BytesPartialDecoderTraits::partial_decode(self, decoded_regions, options)
    }
}
