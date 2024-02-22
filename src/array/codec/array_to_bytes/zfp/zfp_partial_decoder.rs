use zfp_sys::zfp_type;

use crate::{
    array::{
        codec::{ArrayPartialDecoderTraits, BytesPartialDecoderTraits, CodecError, CodecOptions},
        ChunkRepresentation,
    },
    array_subset::ArraySubset,
    byte_range::extract_byte_ranges_concat,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

use super::{zarr_data_type_to_zfp_data_type, zfp_decode, ZfpMode};

/// Partial decoder for the `zfp` codec.
pub struct ZfpPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    mode: ZfpMode,
    zfp_type: zfp_type,
}

impl<'a> ZfpPartialDecoder<'a> {
    /// Create a new partial decoder for the `zfp` codec.
    pub fn new(
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        mode: ZfpMode,
    ) -> Result<Self, CodecError> {
        zarr_data_type_to_zfp_data_type(decoded_representation.data_type()).map_or_else(
            || {
                Err(CodecError::from(
                    "data type {} is unsupported for zfp codec",
                ))
            },
            |zfp_type| {
                Ok(Self {
                    input_handle,
                    decoded_representation: decoded_representation.clone(),
                    mode,
                    zfp_type,
                })
            },
        )
    }
}

impl ArrayPartialDecoderTraits for ZfpPartialDecoder<'_> {
    fn element_size(&self) -> usize {
        self.decoded_representation.element_size()
    }

    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        for array_subset in decoded_regions {
            if array_subset.dimensionality() != self.decoded_representation.dimensionality() {
                return Err(CodecError::InvalidArraySubsetDimensionalityError(
                    array_subset.clone(),
                    self.decoded_representation.dimensionality(),
                ));
            }
        }

        let encoded_value = self.input_handle.decode(options)?;
        let mut out = Vec::with_capacity(decoded_regions.len());
        let chunk_shape = self.decoded_representation.shape_u64();
        match encoded_value {
            Some(encoded_value) => {
                let decoded_value = zfp_decode(
                    &self.mode,
                    self.zfp_type,
                    encoded_value,
                    &self.decoded_representation,
                    false, // FIXME
                )?;
                for array_subset in decoded_regions {
                    let byte_ranges = unsafe {
                        array_subset.byte_ranges_unchecked(
                            &chunk_shape,
                            self.decoded_representation.element_size(),
                        )
                    };
                    out.push(extract_byte_ranges_concat(&decoded_value, &byte_ranges)?);
                }
            }
            None => {
                for decoded_region in decoded_regions {
                    out.push(
                        self.decoded_representation
                            .fill_value()
                            .as_ne_bytes()
                            .repeat(decoded_region.num_elements_usize()),
                    );
                }
            }
        }
        Ok(out)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `zfp` codec.
pub struct AsyncZfpPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
    mode: ZfpMode,
    zfp_type: zfp_type,
}

#[cfg(feature = "async")]
impl<'a> AsyncZfpPartialDecoder<'a> {
    /// Create a new partial decoder for the `zfp` codec.
    pub fn new(
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: &ChunkRepresentation,
        mode: ZfpMode,
    ) -> Result<Self, CodecError> {
        zarr_data_type_to_zfp_data_type(decoded_representation.data_type()).map_or_else(
            || {
                Err(CodecError::from(
                    "data type {} is unsupported for zfp codec",
                ))
            },
            |zfp_type| {
                Ok(Self {
                    input_handle,
                    decoded_representation: decoded_representation.clone(),
                    mode,
                    zfp_type,
                })
            },
        )
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncZfpPartialDecoder<'_> {
    fn element_size(&self) -> usize {
        self.decoded_representation.element_size()
    }

    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        for array_subset in decoded_regions {
            if array_subset.dimensionality() != self.decoded_representation.dimensionality() {
                return Err(CodecError::InvalidArraySubsetDimensionalityError(
                    array_subset.clone(),
                    self.decoded_representation.dimensionality(),
                ));
            }
        }

        let encoded_value = self.input_handle.decode(options).await?;
        let chunk_shape = self.decoded_representation.shape_u64();
        let mut out = Vec::with_capacity(decoded_regions.len());
        match encoded_value {
            Some(encoded_value) => {
                let decoded_value = zfp_decode(
                    &self.mode,
                    self.zfp_type,
                    encoded_value,
                    &self.decoded_representation,
                    false, // FIXME
                )?;
                for array_subset in decoded_regions {
                    let byte_ranges = unsafe {
                        array_subset.byte_ranges_unchecked(
                            &chunk_shape,
                            self.decoded_representation.element_size(),
                        )
                    };
                    out.push(extract_byte_ranges_concat(&decoded_value, &byte_ranges)?);
                }
            }
            None => {
                for decoded_region in decoded_regions {
                    out.push(
                        self.decoded_representation
                            .fill_value()
                            .as_ne_bytes()
                            .repeat(decoded_region.num_elements_usize()),
                    );
                }
            }
        }
        Ok(out)
    }
}
