use crate::{
    array::{
        chunk_shape_to_array_shape,
        codec::{ArrayPartialDecoderTraits, ArraySubset, BytesPartialDecoderTraits, CodecError},
        ChunkRepresentation, DataType,
    },
    array_subset::IncompatibleArraySubsetAndShapeError,
};

#[cfg(feature = "async")]
use crate::array::codec::{AsyncArrayPartialDecoderTraits, AsyncBytesPartialDecoderTraits};

/// Partial decoder for the `bytes` codec.
pub struct PcodecPartialDecoder<'a> {
    input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
}

impl<'a> PcodecPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Box<dyn BytesPartialDecoderTraits + 'a>,
        decoded_representation: ChunkRepresentation,
    ) -> Self {
        Self {
            input_handle,
            decoded_representation,
        }
    }
}

fn do_partial_decode(
    decoded: Option<Vec<u8>>,
    decoded_regions: &[ArraySubset],
    decoded_representation: &ChunkRepresentation,
) -> Result<Vec<Vec<u8>>, CodecError> {
    let mut decoded_bytes = Vec::with_capacity(decoded_regions.len());
    let chunk_shape = chunk_shape_to_array_shape(decoded_representation.shape());
    match decoded {
        None => {
            for array_subset in decoded_regions {
                let bytes_subset = decoded_representation
                    .fill_value()
                    .as_ne_bytes()
                    .repeat(array_subset.num_elements_usize());
                decoded_bytes.push(bytes_subset);
            }
        }
        Some(decoded_value) => {
            macro_rules! pcodec_partial_decode {
                ( $t:ty ) => {
                    let decoded_chunk = pco::standalone::auto_decompress(decoded_value.as_slice())
                        .map(|bytes| crate::array::transmute_to_bytes_vec::<$t>(bytes))
                        .map_err(|err| CodecError::Other(err.to_string()))?;
                    for array_subset in decoded_regions {
                        let bytes_subset = array_subset
                            .extract_bytes(
                                decoded_chunk.as_slice(),
                                &chunk_shape,
                                decoded_representation.element_size(),
                            )
                            .map_err(|_| {
                                IncompatibleArraySubsetAndShapeError::from((
                                    array_subset.clone(),
                                    decoded_representation.shape_u64(),
                                ))
                            })?;
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
    fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let decoded = self.input_handle.decode_opt(parallel)?;
        do_partial_decode(decoded, decoded_regions, &self.decoded_representation)
    }
}

#[cfg(feature = "async")]
/// Asynchronous partial decoder for the `bytes` codec.
pub struct AsyncPCodecPartialDecoder<'a> {
    input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
    decoded_representation: ChunkRepresentation,
}

#[cfg(feature = "async")]
impl<'a> AsyncPCodecPartialDecoder<'a> {
    /// Create a new partial decoder for the `bytes` codec.
    pub fn new(
        input_handle: Box<dyn AsyncBytesPartialDecoderTraits + 'a>,
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
    async fn partial_decode_opt(
        &self,
        decoded_regions: &[ArraySubset],
        parallel: bool,
    ) -> Result<Vec<Vec<u8>>, CodecError> {
        let decoded = self.input_handle.decode_opt(parallel).await?;
        do_partial_decode(decoded, decoded_regions, &self.decoded_representation)
    }
}
