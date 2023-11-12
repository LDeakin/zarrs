//! A cache for partial decoders.

use std::marker::PhantomData;

use crate::{
    array::{ArrayRepresentation, MaybeBytes},
    array_subset::InvalidArraySubsetError,
    byte_range::{extract_byte_ranges, ByteRange},
};

use super::{ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError};

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

/// An array partial decoder cache.
pub struct ArrayPartialDecoderCache<'a> {
    decoded_representation: ArrayRepresentation,
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
        decoded_representation: ArrayRepresentation,
        parallel: bool,
    ) -> Result<Self, CodecError> {
        let cache = input_handle
            .partial_decode_opt(
                &[ArraySubset::new_with_shape(
                    decoded_representation.shape().to_vec(),
                )],
                parallel,
            )?
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
        let array_shape = self.decoded_representation.shape();
        let element_size = self.decoded_representation.element_size();
        for array_subset in decoded_regions {
            out.push(
                array_subset
                    .extract_bytes(&self.cache, array_shape, element_size)
                    .map_err(|_| InvalidArraySubsetError)?,
            );
        }
        Ok(out)
    }
}
