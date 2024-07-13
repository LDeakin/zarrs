//! A cache for partial decoders.

use std::{borrow::Cow, marker::PhantomData};

use crate::{
    array::{ChunkRepresentation, DataType},
    array_subset::IncompatibleArraySubsetAndShapeError,
};

use super::{ArrayPartialDecoderTraits, ArraySubset, CodecError, CodecOptions};

#[cfg(feature = "async")]
use super::AsyncArrayPartialDecoderTraits;

/// A cache for an [`ArrayPartialDecoderTraits`] partial decoder.
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
        options: &CodecOptions,
    ) -> Result<Self, CodecError> {
        let cache = input_handle
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
        options: &CodecOptions,
    ) -> Result<ArrayPartialDecoderCache<'a>, CodecError> {
        let cache = input_handle
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
            cache,
            phantom: PhantomData,
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
    ) -> Result<Vec<Cow<'_, [u8]>>, CodecError> {
        let mut out = Vec::with_capacity(decoded_regions.len());
        let array_shape = self.decoded_representation.shape_u64();
        let element_size = self.decoded_representation.element_size();
        for array_subset in decoded_regions {
            out.push(Cow::Owned(
                array_subset
                    .extract_bytes(&self.cache, &array_shape, element_size)
                    .map_err(|_| {
                        IncompatibleArraySubsetAndShapeError::from((
                            array_subset.clone(),
                            self.decoded_representation.shape_u64(),
                        ))
                    })?,
            ));
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
    ) -> Result<Vec<Cow<'_, [u8]>>, CodecError> {
        ArrayPartialDecoderTraits::partial_decode_opt(self, decoded_regions, options)
    }
}
