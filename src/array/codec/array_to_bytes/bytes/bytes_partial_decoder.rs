use std::sync::Arc;

use crate::{
    array::{
        codec::{
            ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError,
            CodecOptions,
        },
        ArrayBytes, ArraySize, ChunkRepresentation, DataType, DataTypeSize,
    },
    array_subset::IncompatibleArraySubsetAndShapeError,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{reverse_endianness, Endianness};

/// Partial decoder for the `bytes` codec.
pub struct BytesPartialDecoder<'a> {
    input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    endian: Option<Endianness>,
}

impl<'a> BytesPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        endian: Option<Endianness>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            endian,
        }
    }
}

impl ArrayPartialDecoderTraits for BytesPartialDecoder<'_> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        let mut bytes = Vec::with_capacity(decoded_regions.len());
        let chunk_shape = self.decoded_representation.shape_u64();
        for array_subset in decoded_regions {
            match self.decoded_representation.data_type().size() {
                DataTypeSize::Variable => {
                    return Err(CodecError::UnsupportedDataType(
                        self.data_type().clone(),
                        super::IDENTIFIER.to_string(),
                    ))
                }
                DataTypeSize::Fixed(data_type_size) => {
                    // Get byte ranges
                    let byte_ranges = array_subset
                        .byte_ranges(&chunk_shape, data_type_size)
                        .map_err(|_| {
                            IncompatibleArraySubsetAndShapeError::from((
                                array_subset.clone(),
                                self.decoded_representation.shape_u64(),
                            ))
                        })?;

                    // Decode
                    let decoded = self
                        .input_handle
                        .partial_decode_concat(&byte_ranges, options)?
                        .map_or_else(
                            || {
                                let array_size = ArraySize::new(
                                    self.decoded_representation.data_type().size(),
                                    array_subset.num_elements(),
                                );
                                ArrayBytes::new_fill_value(
                                    array_size,
                                    self.decoded_representation.fill_value(),
                                )
                            },
                            |mut decoded| {
                                if let Some(endian) = &self.endian {
                                    if !endian.is_native() {
                                        reverse_endianness(
                                            decoded.to_mut(),
                                            self.decoded_representation.data_type(),
                                        );
                                    }
                                }
                                ArrayBytes::from(decoded)
                            },
                        );

                    bytes.push(decoded);
                }
            }
        }
        Ok(bytes)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `bytes` codec.
pub struct AsyncBytesPartialDecoder<'a> {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    endian: Option<Endianness>,
}

#[cfg(feature = "async")]
impl<'a> AsyncBytesPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
        endian: Option<Endianness>,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
            endian,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncBytesPartialDecoder<'_> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        for array_subset in decoded_regions {
            if array_subset.dimensionality() != self.decoded_representation.dimensionality() {
                return Err(CodecError::InvalidArraySubsetDimensionalityError(
                    array_subset.clone(),
                    self.decoded_representation.dimensionality(),
                ));
            }
        }

        let mut bytes = Vec::with_capacity(decoded_regions.len());
        let chunk_shape = self.decoded_representation.shape_u64();
        for array_subset in decoded_regions {
            if array_subset.dimensionality() != self.decoded_representation.dimensionality() {
                return Err(CodecError::InvalidArraySubsetDimensionalityError(
                    array_subset.clone(),
                    self.decoded_representation.dimensionality(),
                ));
            }

            // Get byte ranges
            let byte_ranges = match self.decoded_representation.element_size() {
                DataTypeSize::Variable => {
                    return Err(CodecError::UnsupportedDataType(
                        self.data_type().clone(),
                        super::IDENTIFIER.to_string(),
                    ))
                }
                DataTypeSize::Fixed(data_type_size) => array_subset
                    .byte_ranges(&chunk_shape, data_type_size)
                    .map_err(|_| {
                        IncompatibleArraySubsetAndShapeError::from((
                            array_subset.clone(),
                            self.decoded_representation.shape_u64(),
                        ))
                    })?,
            };

            // Decode
            let decoded = self
                .input_handle
                .partial_decode_concat(&byte_ranges, options)
                .await?
                .map_or_else(
                    || {
                        let array_size = ArraySize::new(
                            self.decoded_representation.data_type().size(),
                            array_subset.num_elements(),
                        );
                        ArrayBytes::new_fill_value(
                            array_size,
                            self.decoded_representation.fill_value(),
                        )
                    },
                    |mut decoded| {
                        if let Some(endian) = &self.endian {
                            if !endian.is_native() {
                                reverse_endianness(
                                    decoded.to_mut(),
                                    self.decoded_representation.data_type(),
                                );
                            }
                        }
                        ArrayBytes::from(decoded)
                    },
                );

            bytes.push(decoded);
        }
        Ok(bytes)
    }
}
