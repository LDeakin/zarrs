use thiserror::Error;

use crate::{
    array::codec::{
        blosc::{codec_blosc_v2_numcodecs_to_v3, BloscCodecConfigurationNumcodecs},
        bytes::BytesCodecConfigurationV1,
        transpose::{TransposeCodecConfigurationV1, TransposeOrder},
        zstd::codec_zstd_v2_numcodecs_to_v3,
    },
    codec::{blosc::BloscShuffleModeNumcodecs, zstd::ZstdCodecConfiguration},
    v2::{
        array::{
            data_type_metadata_v2_to_endianness, ArrayMetadataV2Order, DataTypeMetadataV2,
            DataTypeMetadataV2InvalidEndiannessError, FillValueMetadataV2,
        },
        ArrayMetadataV2, GroupMetadataV2, MetadataV2,
    },
    v3::{
        array::{
            chunk_grid::regular::RegularChunkGridConfiguration,
            chunk_key_encoding::v2::V2ChunkKeyEncodingConfiguration, data_type::DataTypeSize,
            fill_value::FillValueMetadataV3,
        },
        ArrayMetadataV3, GroupMetadataV3, MetadataV3,
    },
    Endianness, ExtensionAliasesCodecV2, ExtensionAliasesCodecV3, ExtensionAliasesDataTypeV2,
    ExtensionAliasesDataTypeV3,
};

use super::v3::array::data_type::DataTypeMetadataV3;

/// Convert Zarr V2 group metadata to V3.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn group_metadata_v2_to_v3(group_metadata_v2: &GroupMetadataV2) -> GroupMetadataV3 {
    GroupMetadataV3::new()
        .with_attributes(group_metadata_v2.attributes.clone())
        .with_additional_fields(group_metadata_v2.additional_fields.clone())
}

/// An error converting Zarr V2 array metadata to V3.
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

/// Convert Zarr V2 codec metadata to the equivalent Zarr V3 codec metadata.
///
/// # Errors
/// Returns a [`ArrayMetadataV2ToV3ConversionError`] if the metadata is invalid or is not compatible with Zarr V3 metadata.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub fn codec_metadata_v2_to_v3(
    order: ArrayMetadataV2Order,
    dimensionality: usize,
    data_type: &DataTypeMetadataV3,
    endianness: Option<Endianness>,
    filters: &Option<Vec<MetadataV2>>,
    compressor: &Option<MetadataV2>,
    codec_aliases_v2: &ExtensionAliasesCodecV2,
    codec_aliases_v3: &ExtensionAliasesCodecV3,
) -> Result<Vec<MetadataV3>, ArrayMetadataV2ToV3ConversionError> {
    let mut codecs: Vec<MetadataV3> = vec![];

    // Array-to-array codecs
    if order == ArrayMetadataV2Order::F {
        let transpose_metadata = MetadataV3::new_with_serializable_configuration(
            crate::array::codec::transpose::IDENTIFIER.to_string(),
            &TransposeCodecConfigurationV1 {
                order: {
                    let f_order: Vec<usize> = (0..dimensionality).rev().collect();
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
    if let Some(filters) = filters {
        for filter in filters {
            let identifier = codec_aliases_v2.identifier(filter.id());
            let name = codec_aliases_v3.default_name(identifier).to_string();
            match identifier {
                crate::array::codec::vlen_array::IDENTIFIER
                | crate::array::codec::vlen_bytes::IDENTIFIER
                | crate::array::codec::vlen_utf8::IDENTIFIER => {
                    has_array_to_bytes = true;
                    let vlen_v2_metadata =
                        MetadataV3::new_with_configuration(name, serde_json::Map::default());
                    codecs.push(vlen_v2_metadata);
                }
                _ => {
                    codecs.push(MetadataV3::new_with_configuration(
                        name,
                        filter.configuration().clone(),
                    ));
                }
            }
        }
    }

    // Compressor (array to bytes codec)
    if let Some(compressor) = compressor {
        let identifier = codec_aliases_v2.identifier(compressor.id());
        let name = codec_aliases_v3.default_name(identifier).to_string();
        match identifier {
            crate::array::codec::zfpy::IDENTIFIER | crate::array::codec::pcodec::IDENTIFIER => {
                // zfpy / pcodec are v2/v3 compatible
                has_array_to_bytes = true;
                codecs.push(MetadataV3::new_with_configuration(
                    name,
                    compressor.configuration().clone(),
                ));
            }
            _ => {}
        }
    }

    if !has_array_to_bytes {
        let bytes_metadata = MetadataV3::new_with_serializable_configuration(
            crate::array::codec::bytes::IDENTIFIER.to_string(),
            &BytesCodecConfigurationV1 {
                endian: Some(endianness.unwrap_or(Endianness::native())),
            },
        )?;
        codecs.push(bytes_metadata);
    }

    // Compressor (bytes to bytes codec)
    if let Some(compressor) = compressor {
        let identifier = codec_aliases_v2.identifier(compressor.id());
        let name = codec_aliases_v3.default_name(identifier).to_string();
        match identifier {
            crate::array::codec::zfpy::IDENTIFIER | crate::array::codec::pcodec::IDENTIFIER => {
                // already handled above
            }
            crate::array::codec::blosc::IDENTIFIER => {
                let blosc = serde_json::from_value::<BloscCodecConfigurationNumcodecs>(
                    serde_json::to_value(compressor.configuration())?,
                )?;

                let data_type_size = if blosc.shuffle == BloscShuffleModeNumcodecs::NoShuffle {
                    // The data type size does not matter
                    None
                } else {
                    // Special case for known Zarr V2 data types
                    type M = DataTypeMetadataV3;
                    match data_type {
                        M::Bool | M::Int8 | M::UInt8 => Some(DataTypeSize::Fixed(1)),
                        M::Int16 | M::UInt16 | M::Float16 | M::BFloat16 => {
                            Some(DataTypeSize::Fixed(2))
                        }
                        M::Int32 | M::UInt32 | M::Float32 => Some(DataTypeSize::Fixed(4)),
                        M::Int64 | M::UInt64 | M::Float64 | M::Complex64 => {
                            Some(DataTypeSize::Fixed(8))
                        }
                        M::Complex128 => Some(DataTypeSize::Fixed(16)),
                        M::RawBits(size) => Some(DataTypeSize::Fixed(*size)),
                        M::String | M::Bytes => Some(DataTypeSize::Variable),
                        M::Extension(_) => {
                            // In this case the metadata will not match how the data is encoded, but it can still be decoded just fine.
                            // Resaving the array metadata as v3 will not have optimal blosc encoding parameters
                            None
                        }
                    }
                };

                let configuration = codec_blosc_v2_numcodecs_to_v3(&blosc, data_type_size);
                codecs.push(MetadataV3::new_with_serializable_configuration(
                    name,
                    &configuration,
                )?);
            }
            crate::array::codec::zstd::IDENTIFIER => {
                let zstd = serde_json::from_value::<ZstdCodecConfiguration>(serde_json::to_value(
                    compressor.configuration(),
                )?)?;
                let configuration = codec_zstd_v2_numcodecs_to_v3(&zstd);
                codecs.push(MetadataV3::new_with_serializable_configuration(
                    name,
                    &configuration,
                )?);
            }
            _ => codecs.push(MetadataV3::new_with_configuration(
                name,
                compressor.configuration().clone(),
            )),
        }
    }

    Ok(codecs)
}

/// Convert Zarr V2 array metadata to V3.
///
/// # Errors
/// Returns a [`ArrayMetadataV2ToV3ConversionError`] if the metadata is invalid or is not compatible with Zarr V3 metadata.
#[allow(clippy::too_many_lines)]
pub fn array_metadata_v2_to_v3(
    array_metadata_v2: &ArrayMetadataV2,
    codec_aliases_v2: &ExtensionAliasesCodecV2,
    codec_aliases_v3: &ExtensionAliasesCodecV3,
    data_type_aliases_v2: &ExtensionAliasesDataTypeV2,
    data_type_aliases_v3: &ExtensionAliasesDataTypeV3,
) -> Result<ArrayMetadataV3, ArrayMetadataV2ToV3ConversionError> {
    let shape = array_metadata_v2.shape.clone();
    let chunk_grid = MetadataV3::new_with_serializable_configuration(
        crate::v3::array::chunk_grid::regular::IDENTIFIER.to_string(),
        &RegularChunkGridConfiguration {
            chunk_shape: array_metadata_v2.chunks.clone(),
        },
    )?;

    let (Ok(data_type), endianness) = (
        data_type_metadata_v2_to_v3_data_type(
            &array_metadata_v2.dtype,
            data_type_aliases_v2,
            data_type_aliases_v3,
        ),
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
        .or_else(|| {
            // Support zarr-python encoded string arrays with a `null` fill value
            match data_type.name().as_str() {
                "string" => Some(FillValueMetadataV3::from("")),
                _ => None,
            }
        })
        .ok_or_else(|| {
            // TODO: How best to deal with null fill values? What do other implementations do?
            ArrayMetadataV2ToV3ConversionError::UnsupportedFillValue(
                data_type.to_string(),
                array_metadata_v2.fill_value.clone(),
            )
        })?;
    if data_type.name() == "bool" {
        // Map a 0/1 scalar fill value to a bool
        match fill_value.as_u64() {
            Some(0) => fill_value = FillValueMetadataV3::from(false),
            Some(1) => fill_value = FillValueMetadataV3::from(true),
            Some(_) => {
                return Err(ArrayMetadataV2ToV3ConversionError::UnsupportedFillValue(
                    data_type.to_string(),
                    array_metadata_v2.fill_value.clone(),
                ))
            }
            None => {}
        }
    } else if data_type.name() == "string" {
        // Add a special case for `zarr-python` string data with a 0 fill value -> empty string
        if let Some(0) = fill_value.as_u64() {
            fill_value = FillValueMetadataV3::from("");
        }
    }

    let codecs = codec_metadata_v2_to_v3(
        array_metadata_v2.order,
        array_metadata_v2.shape.len(),
        &data_type,
        endianness,
        &array_metadata_v2.filters,
        &array_metadata_v2.compressor,
        codec_aliases_v2,
        codec_aliases_v3,
    )?;

    let chunk_key_encoding = MetadataV3::new_with_serializable_configuration(
        crate::v3::array::chunk_key_encoding::v2::IDENTIFIER.to_string(),
        &V2ChunkKeyEncodingConfiguration {
            separator: array_metadata_v2.dimension_separator,
        },
    )?;

    let attributes = array_metadata_v2.attributes.clone();

    Ok(
        ArrayMetadataV3::new(shape, chunk_grid, data_type, fill_value, codecs)
            .with_attributes(attributes)
            .with_additional_fields(array_metadata_v2.additional_fields.clone())
            .with_chunk_key_encoding(chunk_key_encoding),
    )
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
    data_type_aliases_v2: &ExtensionAliasesDataTypeV2,
    data_type_aliases_v3: &ExtensionAliasesDataTypeV3,
) -> Result<DataTypeMetadataV3, DataTypeMetadataV2UnsupportedDataTypeError> {
    match data_type {
        DataTypeMetadataV2::Simple(name) => {
            let identifier = data_type_aliases_v2.identifier(name);
            let name = data_type_aliases_v3.default_name(identifier).to_string();
            Ok(DataTypeMetadataV3::from_metadata(MetadataV3::new(name)))
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
        FillValueMetadataV2::NaN => Some(f32::NAN.into()),
        FillValueMetadataV2::Infinity => Some(f32::INFINITY.into()),
        FillValueMetadataV2::NegInfinity => Some(f32::NEG_INFINITY.into()),
        FillValueMetadataV2::Number(number) => Some(number.clone().into()),
        FillValueMetadataV2::String(string) => Some(string.clone().into()),
    }
}
