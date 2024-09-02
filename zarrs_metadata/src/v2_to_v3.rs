use thiserror::Error;

use crate::{
    v2::{
        array::{
            codec::{
                blosc::{codec_blosc_v2_numcodecs_to_v3, BloscCodecConfigurationNumcodecs},
                zfpy::{codec_zfpy_v2_numcodecs_to_v3, ZfpyCodecConfigurationNumcodecs},
            },
            data_type_metadata_v2_to_endianness, ArrayMetadataV2Order, DataTypeMetadataV2,
            DataTypeMetadataV2InvalidEndiannessError, FillValueMetadataV2,
        },
        ArrayMetadataV2, GroupMetadataV2,
    },
    v3::{
        array::{
            chunk_grid::regular::RegularChunkGridConfiguration,
            chunk_key_encoding::v2::V2ChunkKeyEncodingConfiguration,
            codec::{
                bytes::BytesCodecConfigurationV1,
                transpose::{TransposeCodecConfigurationV1, TransposeOrder},
                vlen_v2::VlenV2CodecConfigurationV1,
            },
            fill_value::{FillValueFloat, FillValueFloatStringNonFinite, FillValueMetadataV3},
        },
        AdditionalFields, ArrayMetadataV3, GroupMetadataV3, MetadataV3,
    },
};

use super::v3::array::data_type::DataTypeMetadataV3;

/// Convert Zarr V2 group metadata to V3.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn group_metadata_v2_to_v3(group_metadata_v2: &GroupMetadataV2) -> GroupMetadataV3 {
    GroupMetadataV3::new(
        group_metadata_v2.attributes.clone(),
        group_metadata_v2.additional_fields.clone(),
    )
}

/// An error conerting Zarr V3 array metadata to V3.
#[derive(Debug, Error)]
pub enum ArrayMetadataV2ToV3ConversionError {
    /// Unsupported data type.
    #[error("unsupported data type {_0:?}")]
    UnsupportedDataType(String),
    /// Invalid data type endianness.
    #[error(transparent)]
    InvalidEndianness(DataTypeMetadataV2InvalidEndiannessError),
    /// An unsupported codec.
    #[error("unsupported codec {_0} with configuration {_1:?}")]
    UnsupportedCodec(String, serde_json::Map<String, serde_json::Value>),
    /// An unsupported fill value.
    #[error("unsupported fill value {_1:?} for data type {_0}")]
    UnsupportedFillValue(String, FillValueMetadataV2),
    /// Serialization/deserialization error.
    #[error("JSON serialization or deserialization error: {_0}")]
    SerdeError(#[from] serde_json::Error),
    /// Other.
    #[error("{_0}")]
    Other(String),
}

/// Convert Zarr V2 array metadata to V3.
///
/// # Errors
/// Returns a [`ArrayMetadataV2ToV3ConversionError`] if the metadata is invalid or is not compatible with Zarr V3 metadata.
#[allow(clippy::too_many_lines)]
pub fn array_metadata_v2_to_v3(
    array_metadata_v2: &ArrayMetadataV2,
) -> Result<ArrayMetadataV3, ArrayMetadataV2ToV3ConversionError> {
    let shape = array_metadata_v2.shape.clone();
    let chunk_grid = MetadataV3::new_with_serializable_configuration(
        crate::v3::array::chunk_grid::regular::IDENTIFIER,
        &RegularChunkGridConfiguration {
            chunk_shape: array_metadata_v2.chunks.clone(),
        },
    )?;

    let (Ok(data_type), endianness) = (
        data_type_metadata_v2_to_v3_data_type(&array_metadata_v2.dtype),
        data_type_metadata_v2_to_endianness(&array_metadata_v2.dtype)
            .map_err(ArrayMetadataV2ToV3ConversionError::InvalidEndianness)?,
    ) else {
        return Err(ArrayMetadataV2ToV3ConversionError::UnsupportedDataType(
            match &array_metadata_v2.dtype {
                DataTypeMetadataV2::Simple(dtype) => dtype.clone(),
                DataTypeMetadataV2::Structured(dtype) => {
                    return Err(ArrayMetadataV2ToV3ConversionError::UnsupportedDataType(
                        format!("{dtype:?}"),
                    ))
                }
            },
        ));
    };

    // Fill value
    let mut fill_value = array_metadata_fill_value_v2_to_v3(&array_metadata_v2.fill_value)
        .ok_or_else(|| {
            // TODO: How best to deal with null fill values? What do other implementations do?
            ArrayMetadataV2ToV3ConversionError::UnsupportedFillValue(
                data_type.to_string(),
                array_metadata_v2.fill_value.clone(),
            )
        })?;
    if data_type.name() == "bool" {
        // Map a 0/1 scalar fill value to a bool
        if let Some(fill_value_uint) = fill_value.try_as_uint::<u64>() {
            if fill_value_uint == 0 {
                fill_value = FillValueMetadataV3::Bool(false);
            } else if fill_value_uint == 1 {
                fill_value = FillValueMetadataV3::Bool(true);
            } else {
                return Err(ArrayMetadataV2ToV3ConversionError::UnsupportedFillValue(
                    data_type.to_string(),
                    array_metadata_v2.fill_value.clone(),
                ));
            }
        }
    }

    let mut codecs: Vec<MetadataV3> = vec![];

    // Array-to-array codecs
    if array_metadata_v2.order == ArrayMetadataV2Order::F {
        let transpose_metadata = MetadataV3::new_with_serializable_configuration(
            crate::v3::array::codec::transpose::IDENTIFIER,
            &TransposeCodecConfigurationV1 {
                order: {
                    let f_order: Vec<usize> = (0..array_metadata_v2.shape.len()).rev().collect();
                    unsafe {
                        // SAFETY: f_order is valid
                        TransposeOrder::new(&f_order).unwrap_unchecked()
                    }
                },
            },
        )?;
        codecs.push(transpose_metadata);
    }

    // Filters (array to array or array to bytes codecs)
    let mut has_array_to_bytes = false;
    if let Some(filters) = &array_metadata_v2.filters {
        for filter in filters {
            // TODO: Add a V2 registry with V2 to V3 conversion functions
            match filter.id() {
                "vlen-utf8" | "vlen-bytes" | "vlen-array" => {
                    has_array_to_bytes = true;
                    let vlen_v2_metadata = MetadataV3::new_with_serializable_configuration(
                        crate::v3::array::codec::vlen_v2::IDENTIFIER,
                        &VlenV2CodecConfigurationV1 {},
                    )?;
                    codecs.push(vlen_v2_metadata);
                }
                _ => {
                    codecs.push(MetadataV3::new_with_configuration(
                        filter.id(),
                        filter.configuration().clone(),
                    ));
                }
            }
        }
    }

    // Compressor (array to bytes codec)
    if let Some(compressor) = &array_metadata_v2.compressor {
        #[allow(clippy::single_match)]
        match compressor.id() {
            crate::v2::array::codec::zfpy::IDENTIFIER => {
                has_array_to_bytes = true;
                let zfpy_v2_metadata = serde_json::from_value::<ZfpyCodecConfigurationNumcodecs>(
                    serde_json::to_value(compressor.configuration())?,
                )?;
                let configuration = codec_zfpy_v2_numcodecs_to_v3(&zfpy_v2_metadata)?;
                let zfp_v3_metadata = MetadataV3::new_with_serializable_configuration(
                    crate::v3::array::codec::zfp::IDENTIFIER,
                    &configuration,
                )?;
                codecs.push(zfp_v3_metadata);
            }
            crate::v3::array::codec::pcodec::IDENTIFIER => {
                // pcodec is v2/v3 compatible
                has_array_to_bytes = true;
                codecs.push(MetadataV3::new_with_configuration(
                    compressor.id(),
                    compressor.configuration().clone(),
                ));
            }
            _ => {}
        }
    }

    if !has_array_to_bytes {
        let bytes_metadata = MetadataV3::new_with_serializable_configuration(
            crate::v3::array::codec::bytes::IDENTIFIER,
            &BytesCodecConfigurationV1 { endian: endianness },
        )?;
        codecs.push(bytes_metadata);
    }

    // Compressor (bytes to bytes codec)
    if let Some(compressor) = &array_metadata_v2.compressor {
        match compressor.id() {
            crate::v2::array::codec::zfpy::IDENTIFIER
            | crate::v3::array::codec::pcodec::IDENTIFIER => {
                // already handled above
            }
            crate::v3::array::codec::blosc::IDENTIFIER => {
                let blosc = serde_json::from_value::<BloscCodecConfigurationNumcodecs>(
                    serde_json::to_value(compressor.configuration())?,
                )?;
                let configuration = codec_blosc_v2_numcodecs_to_v3(&blosc, &data_type);
                codecs.push(MetadataV3::new_with_serializable_configuration(
                    crate::v3::array::codec::blosc::IDENTIFIER,
                    &configuration,
                )?);
            }
            _ => codecs.push(MetadataV3::new_with_configuration(
                compressor.id(),
                compressor.configuration().clone(),
            )),
        };
    }

    let chunk_key_encoding = MetadataV3::new_with_serializable_configuration(
        crate::v3::array::chunk_key_encoding::v2::IDENTIFIER,
        &V2ChunkKeyEncodingConfiguration {
            separator: array_metadata_v2.dimension_separator,
        },
    )?;

    let attributes = array_metadata_v2.attributes.clone();

    Ok(ArrayMetadataV3::new(
        shape,
        data_type,
        chunk_grid,
        chunk_key_encoding,
        fill_value,
        codecs,
        attributes,
        vec![],
        None,
        AdditionalFields::default(),
    ))
}

/// An unsupported Zarr V2 data type error.
#[derive(Debug, Error)]
#[error("V2 data type {_0:?} is not supported")]
pub struct DataTypeMetadataV2UnsupportedDataTypeError(DataTypeMetadataV2);

/// Convert a Zarr V2 data type to a compatible V3 data type.
///
/// # Errors
/// Returns a [`DataTypeMetadataV2UnsupportedDataTypeError`] if the data type is not supported.
pub fn data_type_metadata_v2_to_v3_data_type(
    data_type: &DataTypeMetadataV2,
) -> Result<DataTypeMetadataV3, DataTypeMetadataV2UnsupportedDataTypeError> {
    match data_type {
        DataTypeMetadataV2::Simple(data_type_str) => {
            match data_type_str.as_str() {
                "|b1" => Ok(DataTypeMetadataV3::Bool),
                "|i1" => Ok(DataTypeMetadataV3::Int8),
                "<i2" | ">i2" => Ok(DataTypeMetadataV3::Int16),
                "<i4" | ">i4" => Ok(DataTypeMetadataV3::Int32),
                "<i8" | ">i8" => Ok(DataTypeMetadataV3::Int64),
                "|u1" => Ok(DataTypeMetadataV3::UInt8),
                "<u2" | ">u2" => Ok(DataTypeMetadataV3::UInt16),
                "<u4" | ">u4" => Ok(DataTypeMetadataV3::UInt32),
                "<u8" | ">u8" => Ok(DataTypeMetadataV3::UInt64),
                "<f2" | ">f2" => Ok(DataTypeMetadataV3::Float16),
                "<f4" | ">f4" => Ok(DataTypeMetadataV3::Float32),
                "<f8" | ">f8" => Ok(DataTypeMetadataV3::Float64),
                "<c8" | ">c8" => Ok(DataTypeMetadataV3::Complex64),
                "<c16" | ">c16" => Ok(DataTypeMetadataV3::Complex128),
                "|O" => Ok(DataTypeMetadataV3::String), // LEGACY: This is not part of the spec. The dtype for a PyObject, which is what zarr-python 2 uses for string arrays.
                // TODO "|mX" timedelta
                // TODO "|MX" datetime
                // TODO "|SX" string (fixed length sequence of char)
                // TODO "|UX" string (fixed length sequence of Py_UNICODE)
                // TODO "|VX" other (void * – each item is a fixed-size chunk of memory)
                _ => Err(DataTypeMetadataV2UnsupportedDataTypeError(
                    data_type.clone(),
                )),
            }
        }
        DataTypeMetadataV2::Structured(_) => Err(DataTypeMetadataV2UnsupportedDataTypeError(
            data_type.clone(),
        )),
    }
}

/// Convert Zarr V2 fill value metadata to [`FillValueMetadataV3`].
///
/// Returns [`None`] for [`FillValueMetadataV2::Null`].
#[must_use]
pub fn array_metadata_fill_value_v2_to_v3(
    fill_value: &FillValueMetadataV2,
) -> Option<FillValueMetadataV3> {
    match fill_value {
        FillValueMetadataV2::Null => None,
        FillValueMetadataV2::NaN => Some(FillValueMetadataV3::Float(FillValueFloat::NonFinite(
            FillValueFloatStringNonFinite::NaN,
        ))),
        FillValueMetadataV2::Infinity => Some(FillValueMetadataV3::Float(
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::PosInfinity),
        )),
        FillValueMetadataV2::NegInfinity => Some(FillValueMetadataV3::Float(
            FillValueFloat::NonFinite(FillValueFloatStringNonFinite::NegInfinity),
        )),
        FillValueMetadataV2::Number(number) => {
            if let Some(u) = number.as_u64() {
                Some(FillValueMetadataV3::UInt(u))
            } else if let Some(i) = number.as_i64() {
                Some(FillValueMetadataV3::Int(i))
            } else if let Some(f) = number.as_f64() {
                Some(FillValueMetadataV3::Float(FillValueFloat::Float(f)))
            } else {
                unreachable!("number must be convertible to u64, i64 or f64")
            }
        }
    }
}
