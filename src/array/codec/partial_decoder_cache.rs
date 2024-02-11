//! A cache for partial decoders.

// TODO: Move BytesPartialDecoderCache and ArrayPartialDecoderCache into separate files

use std::marker::PhantomData;

use crate::{
    array::{ChunkRepresentation, MaybeBytes},
    array_subset::IncompatibleArraySubsetAndShapeError,
    byte_range::{extract_byte_ranges, ByteRange},
};

use super::{
    ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError,
    PartialDecodeOptions,
};

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
        options: &PartialDecodeOptions,
    ) -> Result<Self, CodecError> {
        let cache = input_handle
            .partial_decode_opt(&[ByteRange::FromStart(0, None)], options)?
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
        options: &PartialDecodeOptions,
    ) -> Result<BytesPartialDecoderCache<'a>, CodecError> {
        let cache = input_handle
            .partial_decode_opt(&[ByteRange::FromStart(0, None)], options)
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
        _options: &PartialDecodeOptions,
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
#[async_trait::async_trait]
impl AsyncBytesPartialDecoderTraits for BytesPartialDecoderCache<'_> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ByteRange],
        options: &PartialDecodeOptions,
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        BytesPartialDecoderTraits::partial_decode_opt(self, decoded_regions, options)
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
        options: &PartialDecodeOptions,
    ) -> Result<Self, CodecError> {
        let cache = input_handle
            .partial_decode_opt(
                &[ArraySubset::new_with_shape(
                    decoded_representation.shape_u64(),
                )],
                options,
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
        options: &PartialDecodeOptions,
    ) -> Result<ArrayPartialDecoderCache<'a>, CodecError> {
        let cache = input_handle
            .partial_decode_opt(
                &[ArraySubset::new_with_shape(
                    decoded_representation.shape_u64(),
                )],
                options,
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
        _options: &PartialDecodeOptions,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(decoded_regions.len());
        let array_shape = self.decoded_representation.shape_u64();
        let element_size = self.decoded_representation.element_size();
        for array_subset in decoded_regions {
            out.push(
                array_subset
                    .extract_bytes(&self.cache, &array_shape, element_size)
                    .map_err(|_| {
                        IncompatibleArraySubsetAndShapeError::from((
                            array_subset.clone(),
                            self.decoded_representation.shape_u64(),
                        ))
                    })?,
            );
        }
        Ok(out)
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<'a> AsyncArrayPartialDecoderTraits for ArrayPartialDecoderCache<'a> {
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &PartialDecodeOptions,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        ArrayPartialDecoderTraits::partial_decode_opt(self, decoded_regions, options)
    }
}
