//! A cache for partial decoders.

use std::marker::PhantomData;

use crate::{
    array::{chunk_shape_to_array_shape, ChunkRepresentation, MaybeBytes},
    array_subset::InvalidArraySubsetError,
    byte_range::{extract_byte_ranges, ByteRange},
};

use super::{ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError};

#[cfg(feature = "async")]
use super::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

/// A bytes partial decoder cache.
pub struct BytesPartialDecoderCache<'a> {
    cache: MaybeBytes,
    phantom: PhantomData<&'a ()>,
}

impl<'a> BytesPartialDecoderCache<'a> {
    /// Create a new partial decoder cache.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if caching fails.
    pub fn new(
        input_handle: &dyn BytesPartialDecoderTraits,
        parallel: bool,
    ) -> Result<Self, CodecError> {
        let cache = input_handle
            .partial_decode_opt(&[ByteRange::FromStart(0, None)], parallel)?
            .map(|mut bytes| bytes.remove(0));
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
        parallel: bool,
    ) -> Result<BytesPartialDecoderCache<'a>, CodecError> {
        let cache = input_handle
            .partial_decode_opt(&[ByteRange::FromStart(0, None)], parallel)
            .await?
            .map(|mut bytes| bytes.remove(0));
        Ok(Self {
            cache,
            phantom: PhantomData,
        })
    }
}

impl BytesPartialDecoderTraits for BytesPartialDecoderCache<'_> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        _parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        Ok(match &self.cache {
            Some(bytes) => Some(
                extract_byte_ranges(bytes, decoded_regions)
                    .map_err(CodecError::InvalidByteRangeError)?,
            ),
            None => None,
        })
    }
}

#[cfg(feature = "async")]
#[cfg_attr(feature = "async", async_trait::async_trait)]
impl AsyncBytesPartialDecoderTraits for BytesPartialDecoderCache<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        parallel: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        BytesPartialDecoderTraits::partial_decode_opt(self, decoded_regions, parallel)
    }
}

/// An array partial decoder cache.
pub struct ArrayPartialDecoderCache<'a> {
    decoded_representation: ChunkRepresentation,
    cache: Vec<u8>,
    phantom: PhantomData<&'a ()>,
}

impl<'a> ArrayPartialDecoderCache<'a> {
    /// Create a new partial decoder cache.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation of the partial decoder fails.
    pub fn new(
        input_handle: &dyn ArrayPartialDecoderTraits,
        decoded_representation: ChunkRepresentation,
        parallel: bool,
    ) -> Result<Self, CodecError> {
        let cache = input_handle
            .partial_decode_opt(
                &[ArraySubset::new_with_shape(chunk_shape_to_array_shape(
                    decoded_representation.shape(),
                ))],
                parallel,
            )?
            .remove(0);
        Ok(Self {
            decoded_representation,
            cache,
            phantom: PhantomData,
        })
    }

    #[cfg(feature = "async")]
    /// Create a new asynchronous partial decoder cache.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation of the partial decoder fails.
    pub async fn async_new(
        input_handle: &dyn AsyncArrayPartialDecoderTraits,
        decoded_representation: ChunkRepresentation,
        parallel: bool,
    ) -> Result<ArrayPartialDecoderCache<'a>, CodecError> {
        let cache = input_handle
            .partial_decode_opt(
                &[ArraySubset::new_with_shape(chunk_shape_to_array_shape(
                    decoded_representation.shape(),
                ))],
                parallel,
            )
            .await?
            .remove(0);
        Ok(Self {
            decoded_representation,
            cache,
            phantom: PhantomData,
        })
    }
}

impl<'a> ArrayPartialDecoderTraits for ArrayPartialDecoderCache<'a> {
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        _parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(decoded_regions.len());
        let array_shape = chunk_shape_to_array_shape(self.decoded_representation.shape());
        let element_size = self.decoded_representation.element_size();
        for array_subset in decoded_regions {
            out.push(
                array_subset
                    .extract_bytes(&self.cache, &array_shape, element_size)
                    .map_err(|_| InvalidArraySubsetError)?,
            );
        }
        Ok(out)
    }
}

#[cfg(feature = "async")]
#[cfg_attr(feature = "async", async_trait::async_trait)]
impl<'a> AsyncArrayPartialDecoderTraits for ArrayPartialDecoderCache<'a> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        ArrayPartialDecoderTraits::partial_decode_opt(self, decoded_regions, parallel)
    }
}
