//! A cache for partial decoders.

use parking_lot::RwLock;

use crate::{
    array::{ArrayRepresentation, BytesRepresentation, MaybeBytes},
    array_subset::InvalidArraySubsetError,
    byte_range::{extract_byte_ranges, ByteRange},
};

use super::{ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError};

/// A bytes partial decoder cache.
pub struct BytesPartialDecoderCache<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    cache: RwLock<Option<MaybeBytes>>,
}

impl<'a> BytesPartialDecoderCache<'a> {
    /// Create a new partial decoder cache.
    #[must_use]
    pub fn new(input_handle: Box<dyn BytesPartialDecoderTraits + 'a>) -> Self {
        Self {
            input_handle,
            cache: RwLock::new(None),
        }
    }
}

impl BytesPartialDecoderTraits for BytesPartialDecoderCache<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &BytesRepresentation,
        decoded_regions: &[ByteRange],
    ) -> Result<Option<Vec<Vec<u8>>>, CodecError> {
        let mut read_cache = self.cache.read();
        if read_cache.is_none() {
            drop(read_cache);
            let mut write_cache = self.cache.write();
            if write_cache.is_none() {
                *write_cache = Some(self.input_handle.decode(decoded_representation)?);
            }
            drop(write_cache);
            read_cache = self.cache.read();
        }
        let bytes = read_cache.as_ref().unwrap();
        Ok(match bytes {
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
    input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>,
    cache: RwLock<Option<Vec<u8>>>,
}

impl<'a> ArrayPartialDecoderCache<'a> {
    /// Create a new partial decoder cache.
    #[must_use]
    pub fn new(input_handle: Box<dyn ArrayPartialDecoderTraits + 'a>) -> Self {
        Self {
            input_handle,
            cache: RwLock::new(None),
        }
    }
}

impl ArrayPartialDecoderTraits for ArrayPartialDecoderCache<'_> {
    fn partial_decode(
        &self,
        decoded_representation: &ArrayRepresentation,
        decoded_regions: &[ArraySubset],
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let mut read_cache = self.cache.read();
        if read_cache.is_none() {
            drop(read_cache);
            let mut write_cache = self.cache.write();
            if write_cache.is_none() {
                *write_cache = Some(self.input_handle.decode(decoded_representation)?);
            }
            drop(write_cache);
            read_cache = self.cache.read();
        }
        let cache = read_cache.as_ref().unwrap();

        let mut out: Vec<Vec<u8>> = Vec::with_capacity(decoded_regions.len());
        let array_shape = decoded_representation.shape();
        let element_size = decoded_representation.element_size();
        for array_subset in decoded_regions {
            out.push(
                array_subset
                    .extract_bytes(cache, array_shape, element_size)
                    .map_err(|_| InvalidArraySubsetError)?,
            );
        }
        Ok(out)
    }
}
