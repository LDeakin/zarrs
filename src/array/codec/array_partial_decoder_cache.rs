//! A cache for partial decoders.

use crate::array::{ArrayBytes, ChunkRepresentation, DataType};

use super::{ArrayPartialDecoderTraits, ArraySubset, CodecError, CodecOptions};

#[cfg(feature = "async")]
use super::AsyncArrayPartialDecoderTraits;

/// A cache for an [`ArrayPartialDecoderTraits`] partial decoder.
pub struct ArrayPartialDecoderCache<'a> {
    decoded_representation: ChunkRepresentation,
    cache: ArrayBytes<'a>,
}

impl<'a> ArrayPartialDecoderCache<'a> {
    /// Create a new partial decoder cache.
    ///
    /// # Errors
    /// Returns a [`CodecError`] if initialisation of the partial decoder fails.
    pub fn new(
        input_handle: &dyn ArrayPartialDecoderTraits,
        decoded_representation: ChunkRepresentation,
        options: &CodecOptions,
    ) -> Result<Self, CodecError> {
        let bytes = input_handle
            .partial_decode_opt(
                &[ArraySubset::new_with_shape(
                    decoded_representation.shape_u64(),
                )],
                options,
            )?
            .remove(0)
            .into_owned();
        Ok(Self {
            decoded_representation,
            cache: bytes,
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
        options: &CodecOptions,
    ) -> Result<ArrayPartialDecoderCache<'a>, CodecError> {
        let bytes = input_handle
            .partial_decode_opt(
                &[ArraySubset::new_with_shape(
                    decoded_representation.shape_u64(),
                )],
                options,
            )
            .await?
            .remove(0)
            .into_owned();
        Ok(Self {
            decoded_representation,
            cache: bytes,
        })
    }
}

impl<'a> ArrayPartialDecoderTraits for ArrayPartialDecoderCache<'a> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        _options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        let mut out = Vec::with_capacity(decoded_regions.len());
        let array_shape = self.decoded_representation.shape_u64();
        for array_subset in decoded_regions {
            out.push(self.cache.extract_array_subset(
                array_subset,
                &array_shape,
                self.decoded_representation.data_type(),
            )?);
        }
        Ok(out)
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<'a> AsyncArrayPartialDecoderTraits for ArrayPartialDecoderCache<'a> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        ArrayPartialDecoderTraits::partial_decode_opt(self, decoded_regions, options)
    }
}
