use std::sync::Arc;

use crate::array::{
    codec::{
        ArrayBytes, ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError,
        CodecOptions, RawBytes,
    },
    ArraySize, ChunkRepresentation, DataType,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

/// Partial decoder for the `bytes` codec.
pub struct PcodecPartialDecoder<'a> {
    input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
}

impl<'a> PcodecPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Arc<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
        }
    }
}

fn do_partial_decode<'a>(
    decoded: Option<RawBytes<'a>>,
    decoded_regions: &[ArraySubset],
    decoded_representation: &ChunkRepresentation,
) -> Result<Vec<ArrayBytes<'a>>, CodecError> {
    let mut decoded_bytes = Vec::with_capacity(decoded_regions.len());
    let chunk_shape = decoded_representation.shape_u64();
    match decoded {
        None => {
            for array_subset in decoded_regions {
                let array_size = ArraySize::new(
                    decoded_representation.data_type().size(),
                    array_subset.num_elements(),
                );
                let fill_value =
                    ArrayBytes::new_fill_value(array_size, decoded_representation.fill_value());
                decoded_bytes.push(fill_value);
            }
        }
        Some(decoded_value) => {
            macro_rules! pcodec_partial_decode {
                ( $t:ty ) => {
                    let decoded_chunk = pco::standalone::simple_decompress(&decoded_value)
                        .map(|bytes| crate::array::transmute_to_bytes_vec::<$t>(bytes))
                        .map_err(|err| CodecError::Other(err.to_string()))?;
                    let decoded_chunk: ArrayBytes = decoded_chunk.into();
                    for array_subset in decoded_regions {
                        let bytes_subset = decoded_chunk
                            .extract_array_subset(
                                array_subset,
                                &chunk_shape,
                                decoded_representation.data_type(),
                            )?
                            .into_owned();
                        decoded_bytes.push(bytes_subset);
                    }
                };
            }

            let data_type = decoded_representation.data_type();
            match data_type {
                DataType::UInt32 => {
                    pcodec_partial_decode!(u32);
                }
                DataType::UInt64 => {
                    pcodec_partial_decode!(u64);
                }
                DataType::Int32 => {
                    pcodec_partial_decode!(i32);
                }
                DataType::Int64 => {
                    pcodec_partial_decode!(i64);
                }
                DataType::Float32 | DataType::Complex64 => {
                    pcodec_partial_decode!(f32);
                }
                DataType::Float64 | DataType::Complex128 => {
                    pcodec_partial_decode!(f64);
                }
                _ => {
                    return Err(CodecError::UnsupportedDataType(
                        data_type.clone(),
                        super::IDENTIFIER.to_string(),
                    ))
                }
            };
        }
    }
    Ok(decoded_bytes)
}

impl ArrayPartialDecoderTraits for PcodecPartialDecoder<'_> {
    fn data_type(&self) -> &DataType {
        self.decoded_representation.data_type()
    }

    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        options: &CodecOptions,
    ) -> Result<Vec<ArrayBytes<'_>>, CodecError> {
        let decoded = self.input_handle.decode(options)?;
        do_partial_decode(decoded, decoded_regions, &self.decoded_representation)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `bytes` codec.
pub struct AsyncPCodecPartialDecoder<'a> {
    input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
}

#[cfg(feature = "async")]
impl<'a> AsyncPCodecPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Arc<dyn AsyncBytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
        }
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncArrayPartialDecoderTraits for AsyncPCodecPartialDecoder<'_> {
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

        let decoded = self.input_handle.decode(options).await?;
        do_partial_decode(decoded, decoded_regions, &self.decoded_representation)
    }
}
