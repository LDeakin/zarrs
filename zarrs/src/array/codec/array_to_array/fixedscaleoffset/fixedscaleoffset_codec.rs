use std::sync::Arc;

use zarrs_metadata::{
    v2::array::DataTypeMetadataV2, v2_to_v3::data_type_metadata_v2_to_v3, v3::MetadataConfiguration,
};
use zarrs_plugin::PluginCreateError;
use zarrs_registry::codec::FIXEDSCALEOFFSET;

use crate::{
    array::{
        codec::{
            ArrayBytes, ArrayCodecTraits, ArrayToArrayCodecTraits, CodecError,
            CodecMetadataOptions, CodecOptions, CodecTraits, RecommendedConcurrency,
        },
        ChunkRepresentation, DataType,
    },
    config::global_config,
};

use super::{FixedScaleOffsetCodecConfiguration, FixedScaleOffsetCodecConfigurationNumcodecs};

/// A `fixedscaleoffset` codec implementation.
#[derive(Clone, Debug)]
pub struct FixedScaleOffsetCodec {
    offset: f32,
    scale: f32,
    dtype_str: String,
    astype_str: Option<String>,
    dtype: DataType,
    astype: Option<DataType>,
}

fn add_byteoder_to_dtype(dtype: &str) -> String {
    if dtype == "u1" {
        "|u1".to_string()
    } else if !(dtype.starts_with('<') | dtype.starts_with('>')) {
        format!("<{dtype}")
    } else {
        dtype.to_string()
    }
}

impl FixedScaleOffsetCodec {
    /// Create a new `fixedscaleoffset` codec from a configuration.
    ///
    /// # Errors
    /// Returns an error if the configuration is not supported.
    pub fn new_with_configuration(
        configuration: &FixedScaleOffsetCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        match configuration {
            FixedScaleOffsetCodecConfiguration::Numcodecs(configuration) => {
                // Add a byteorder to the data type name, byteorder may be omitted
                // FixedScaleOffsets permits `dtype` / `astype` with and without a byteoder character, but it is irrelevant
                let dtype = add_byteoder_to_dtype(&configuration.dtype);
                let astype = configuration
                    .astype
                    .as_ref()
                    .map(|astype| add_byteoder_to_dtype(astype));

                // Get the data type metadata
                let dtype = DataTypeMetadataV2::Simple(dtype);
                let astype = astype
                    .as_ref()
                    .map(|dtype| DataTypeMetadataV2::Simple(dtype.clone()));

                // Convert to a V3 data type
                let dtype_err = |_| {
                    PluginCreateError::Other(
                        "fixedscaleoffset cannot interpret Zarr V2 data type as V3 equivalent"
                            .to_string(),
                    )
                };
                let config = global_config();
                let data_type_aliases_v2 = config.data_type_aliases_v2();
                let data_type_aliases_v3 = config.data_type_aliases_v3();
                let dtype = DataType::from_metadata(
                    &data_type_metadata_v2_to_v3(
                        &dtype,
                        data_type_aliases_v2,
                        data_type_aliases_v3,
                    )
                    .map_err(dtype_err)?,
                    data_type_aliases_v3,
                )?;
                let astype = if let Some(astype) = astype {
                    Some(DataType::from_metadata(
                        &data_type_metadata_v2_to_v3(
                            &astype,
                            data_type_aliases_v2,
                            data_type_aliases_v3,
                        )
                        .map_err(dtype_err)?,
                        data_type_aliases_v3,
                    )?)
                } else {
                    None
                };

                Ok(Self {
                    offset: configuration.offset,
                    scale: configuration.scale,
                    dtype,
                    astype,
                    dtype_str: configuration.dtype.clone(),
                    astype_str: configuration.astype.clone(),
                })
            }
            _ => Err(PluginCreateError::Other(
                "this fixedscaleoffset codec configuration variant is unsupported".to_string(),
            )),
        }
    }
}

impl CodecTraits for FixedScaleOffsetCodec {
    fn identifier(&self) -> &str {
        super::FIXEDSCALEOFFSET
    }

    fn configuration_opt(
        &self,
        _name: &str,
        _options: &CodecMetadataOptions,
    ) -> Option<MetadataConfiguration> {
        let configuration = FixedScaleOffsetCodecConfiguration::Numcodecs(
            FixedScaleOffsetCodecConfigurationNumcodecs {
                offset: self.offset,
                scale: self.scale,
                dtype: self.dtype_str.clone(),
                astype: self.astype_str.clone(),
            },
        );
        Some(configuration.into())
    }

    fn partial_decoder_should_cache_input(&self) -> bool {
        false
    }

    fn partial_decoder_decodes_all(&self) -> bool {
        false
    }
}

impl ArrayCodecTraits for FixedScaleOffsetCodec {
    fn recommended_concurrency(
        &self,
        _decoded_representation: &ChunkRepresentation,
    ) -> Result<RecommendedConcurrency, CodecError> {
        Ok(RecommendedConcurrency::new_maximum(1))
    }
}

macro_rules! scale_data_type {
    ($data_type:expr, $bytes:expr, $offset:expr, $scale:expr, {
        $($variant:ident => $ty:ty, $float:ty),* $(,)?
    }) => {
        match $data_type {
            $(DataType::$variant => {
                let round = |chunk: &mut [u8]| {
                    let element = <$ty>::from_ne_bytes(chunk.try_into().unwrap());
                    let element = ((element as $float - $offset as $float) * $scale as $float).round() as $ty;
                    chunk.copy_from_slice(&element.to_ne_bytes());
                };
                $bytes.chunks_exact_mut(std::mem::size_of::<$ty>()).for_each(round);
                Ok(())
            }),*
            _ => Err(CodecError::UnsupportedDataType(
                $data_type.clone(),
                FIXEDSCALEOFFSET.to_string(),
            )),
        }
    };
}

macro_rules! unscale_data_type {
    ($data_type:expr, $bytes:expr, $offset:expr, $scale:expr, {
        $($variant:ident => $ty:ty, $float:ty),* $(,)?
    }) => {
        match $data_type {
            $(DataType::$variant => {
                let round = |chunk: &mut [u8]| {
                    let element = <$ty>::from_ne_bytes(chunk.try_into().unwrap());
                    let element = ((element as $float / $scale as $float) + $offset as $float) as $ty;
                    chunk.copy_from_slice(&element.to_ne_bytes());
                };
                $bytes.chunks_exact_mut(std::mem::size_of::<$ty>()).for_each(round);
                Ok(())
            }),*
            _ => Err(CodecError::UnsupportedDataType(
                $data_type.clone(),
                FIXEDSCALEOFFSET.to_string(),
            )),
        }
    };
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::cast_sign_loss
)]
fn scale_array(
    bytes: &mut [u8],
    data_type: &DataType,
    offset: f32,
    scale: f32,
) -> Result<(), CodecError> {
    scale_data_type!(data_type, bytes, offset, scale, {
        Int8 => i8, f32,
        Int16 => i16, f32,
        Int32 => i32, f64,
        Int64 => i64, f64,
        UInt8 => u8, f32,
        UInt16 => u16, f32,
        UInt32 => u32, f64,
        UInt64 => u64, f64,
        Float32 => f32, f32,
        Float64 => f64, f64,
    })
    // FIXME: Half types, complex types
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::cast_sign_loss
)]
fn unscale_array(
    bytes: &mut [u8],
    data_type: &DataType,
    offset: f32,
    scale: f32,
) -> Result<(), CodecError> {
    unscale_data_type!(data_type, bytes, offset, scale, {
        Int8 => i8, f32,
        Int16 => i16, f32,
        Int32 => i32, f64,
        Int64 => i64, f64,
        UInt8 => u8, f32,
        UInt16 => u16, f32,
        UInt32 => u32, f64,
        UInt64 => u64, f64,
        Float32 => f32, f32,
        Float64 => f64, f64,
    })
    // FIXME: Half types, complex types
}

macro_rules! cast_to_float {
    ($data_type:expr, $bytes:expr, {
        $($variant:ident => $ty:ty),* $(,)?
    }) => {
        match $data_type {
            $(DataType::$variant => {
                let from_bytes = |chunk: &[u8]| {
                    let element = <$ty>::from_ne_bytes(chunk.try_into().unwrap());
                    element as f32
                };
                Ok($bytes.chunks_exact(std::mem::size_of::<$ty>()).map(from_bytes).collect())
            }),*
            _ => Err(CodecError::UnsupportedDataType(
                $data_type.clone(),
                FIXEDSCALEOFFSET.to_string(),
            )),
        }
    };
}

macro_rules! cast_from_float {
    ($data_type:expr, $float_iter:expr, {
        $($variant:ident => $ty:ty),* $(,)?
    }) => {
        match $data_type {
            $(DataType::$variant => {
                let to_bytes = |element: f32| {
                    let element = element as $ty;
                    let bytes = <$ty>::to_ne_bytes(element);
                    bytes
                };
                Ok($float_iter.map(to_bytes).flatten().collect())
            }),*
            _ => Err(CodecError::UnsupportedDataType(
                $data_type.clone(),
                FIXEDSCALEOFFSET.to_string(),
            )),
        }
    };
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::cast_sign_loss
)]
fn cast_array(
    bytes: &[u8],
    data_type: &DataType,
    as_type: &DataType,
) -> Result<Vec<u8>, CodecError> {
    let elements: Vec<f32> = cast_to_float!(data_type, bytes, {
        Int8 => i8,
        Int16 => i16,
        Int32 => i32,
        Int64 => i64,
        UInt8 => u8,
        UInt16 => u16,
        UInt32 => u32,
        UInt64 => u64,
        Float32 => f32,
        Float64 => f64,
    })?;
    // FIXME: Half types, complex types

    cast_from_float!(as_type, elements.into_iter(), {
        Int8 => i8,
        Int16 => i16,
        Int32 => i32,
        Int64 => i64,
        UInt8 => u8,
        UInt16 => u16,
        UInt32 => u32,
        UInt64 => u64,
        Float32 => f32,
        Float64 => f64,
    })
    // FIXME: Half types, complex types
}

fn do_encode<'a>(
    bytes: ArrayBytes<'a>,
    data_type: &DataType,
    offset: f32,
    scale: f32,
    astype: Option<&DataType>,
) -> Result<ArrayBytes<'a>, CodecError> {
    let mut bytes = bytes.into_fixed()?.into_owned();
    scale_array(&mut bytes, data_type, offset, scale)?;
    if let Some(astype) = astype {
        Ok(cast_array(&bytes, data_type, astype)?.into())
    } else {
        Ok(bytes.into())
    }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ArrayToArrayCodecTraits for FixedScaleOffsetCodec {
    fn into_dyn(self: Arc<Self>) -> Arc<dyn ArrayToArrayCodecTraits> {
        self as Arc<dyn ArrayToArrayCodecTraits>
    }

    fn encode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        if &self.dtype != decoded_representation.data_type() {
            return Err(CodecError::Other(format!(
                "fixedscaleoffset got {} as input, but metadata expects {}",
                decoded_representation.data_type(),
                self.dtype
            )));
        }

        do_encode(
            bytes,
            decoded_representation.data_type(),
            self.offset,
            self.scale,
            self.astype.as_ref(),
        )
    }

    fn decode<'a>(
        &self,
        bytes: ArrayBytes<'a>,
        decoded_representation: &ChunkRepresentation,
        _options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        if &self.dtype != decoded_representation.data_type() {
            return Err(CodecError::Other(format!(
                "fixedscaleoffset got {} as input, but metadata expects {}",
                decoded_representation.data_type(),
                self.dtype
            )));
        }

        let bytes = bytes.into_fixed()?.into_owned();
        let mut bytes = if let Some(astype) = &self.astype {
            cast_array(&bytes, astype, decoded_representation.data_type())?
        } else {
            bytes
        };
        unscale_array(
            &mut bytes,
            decoded_representation.data_type(),
            self.offset,
            self.scale,
        )?;
        Ok(bytes.into())
    }

    fn encoded_data_type(&self, decoded_data_type: &DataType) -> Result<DataType, CodecError> {
        match decoded_data_type {
            DataType::Float16
            | DataType::BFloat16
            | DataType::Float32
            | DataType::Float64
            | DataType::UInt8
            | DataType::Int8
            | DataType::UInt16
            | DataType::Int16
            | DataType::UInt32
            | DataType::Int32
            | DataType::UInt64
            | DataType::Int64
            | DataType::Complex64
            | DataType::Complex128 => {
                if let Some(astype) = &self.astype {
                    Ok(astype.clone())
                } else {
                    Ok(decoded_data_type.clone())
                }
            }
            _ => Err(CodecError::UnsupportedDataType(
                decoded_data_type.clone(),
                FIXEDSCALEOFFSET.to_string(),
            )),
        }
    }
}
