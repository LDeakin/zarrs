use thiserror::Error;

use crate::{
    extension::{
        ExtensionAliasesCodecV2, ExtensionAliasesCodecV3, ExtensionAliasesDataTypeV2,
        ExtensionAliasesDataTypeV3,
    },
    v2::{
        array::{
            data_type_metadata_v2_to_endianness, ArrayMetadataV2Order, DataTypeMetadataV2,
            DataTypeMetadataV2InvalidEndiannessError, FillValueMetadataV2,
        },
        ArrayMetadataV2, GroupMetadataV2, MetadataV2,
    },
    v3::{
        array::{
            chunk_grid::{self, regular::RegularChunkGridConfiguration},
            chunk_key_encoding::{self, v2::V2ChunkKeyEncodingConfiguration},
            codec::{
                self,
                blosc::{
                    codec_blosc_v2_numcodecs_to_v3, BloscCodecConfigurationNumcodecs,
                    BloscShuffleModeNumcodecs,
                },
                bytes::BytesCodecConfigurationV1,
                transpose::{TransposeCodecConfigurationV1, TransposeOrder},
                zstd::{codec_zstd_v2_numcodecs_to_v3, ZstdCodecConfiguration},
            },
            data_type::{self},
            fill_value::FillValueMetadataV3,
        },
        ArrayMetadataV3, GroupMetadataV3, MetadataV3,
    },
    DataTypeSize, Endianness,
};

/// Convert Zarr V2 group metadata to Zarr V3.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn group_metadata_v2_to_v3(group_metadata_v2: &GroupMetadataV2) -> GroupMetadataV3 {
    GroupMetadataV3::new()
        .with_attributes(group_metadata_v2.attributes.clone())
        .with_additional_fields(group_metadata_v2.additional_fields.clone())
}

/// An error converting Zarr V2 array metadata to Zarr V3.
#[derive(Debug, Error)]
pub enum ArrayMetadataV2ToV3ConversionError {
    /// Unsupported data type.
    #[error("unsupported data type {_0:?}")]
    UnsupportedDataType(DataTypeMetadataV2),
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

/// Convert Zarr V2 codec metadata to Zarr V3.
///
/// # Errors
/// Returns a [`ArrayMetadataV2ToV3ConversionError`] if the metadata is invalid or is not compatible with Zarr V3 metadata.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub fn codec_metadata_v2_to_v3(
    order: ArrayMetadataV2Order,
    dimensionality: usize,
    data_type: &MetadataV3,
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
            codec::TRANSPOSE.to_string(),
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
                codec::VLEN_ARRAY | codec::VLEN_BYTES | codec::VLEN_UTF8 => {
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
            codec::ZFPY | codec::PCODEC => {
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
            codec::BYTES.to_string(),
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
            codec::ZFPY | codec::PCODEC => {
                // already handled above
            }
            codec::BLOSC => {
                let blosc = serde_json::from_value::<BloscCodecConfigurationNumcodecs>(
                    serde_json::to_value(compressor.configuration())?,
                )?;

                let data_type_size = if blosc.shuffle == BloscShuffleModeNumcodecs::NoShuffle {
                    // The data type size does not matter
                    None
                } else {
                    // Special case for known Zarr V2 / Zarr V3 compatible data types
                    // If the data type has an unknown size
                    //  - the metadata will not match how the data is encoded, but it can still be decoded just fine
                    //  - resaving the array metadata as v3 will not have optimal blosc encoding parameters
                    match data_type.name() {
                        data_type::BOOL | data_type::INT8 | data_type::UINT8 => {
                            Some(DataTypeSize::Fixed(1))
                        }
                        data_type::INT16
                        | data_type::UINT16
                        | data_type::FLOAT16
                        | data_type::BFLOAT16 => Some(DataTypeSize::Fixed(2)),
                        data_type::INT32 | data_type::UINT32 | data_type::FLOAT32 => {
                            Some(DataTypeSize::Fixed(4))
                        }
                        data_type::INT64
                        | data_type::UINT64
                        | data_type::FLOAT64
                        | data_type::COMPLEX64 => Some(DataTypeSize::Fixed(8)),
                        data_type::COMPLEX128 => Some(DataTypeSize::Fixed(16)),
                        data_type::STRING | data_type::BYTES => Some(DataTypeSize::Variable),
                        name => {
                            // Special case for raw bits data types
                            if name.starts_with('r') && name.len() > 1 {
                                if let Ok(size_bits) = name[1..].parse::<usize>() {
                                    if size_bits % 8 == 0 {
                                        let size_bytes = size_bits / 8;
                                        Some(DataTypeSize::Fixed(size_bytes))
                                    } else {
                                        return Err(
                                            ArrayMetadataV2ToV3ConversionError::UnsupportedDataType(
                                                DataTypeMetadataV2::Simple(name.to_string()),
                                            ),
                                        );
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                    }
                };

                let configuration = codec_blosc_v2_numcodecs_to_v3(&blosc, data_type_size);
                codecs.push(MetadataV3::new_with_serializable_configuration(
                    name,
                    &configuration,
                )?);
            }
            codec::ZSTD => {
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

/// Convert Zarr V2 array metadata to Zarr V3.
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
        chunk_grid::REGULAR.to_string(),
        &RegularChunkGridConfiguration {
            chunk_shape: array_metadata_v2.chunks.clone(),
        },
    )?;

    let (Ok(data_type), endianness) = (
        data_type_metadata_v2_to_v3(
            &array_metadata_v2.dtype,
            data_type_aliases_v2,
            data_type_aliases_v3,
        ),
        data_type_metadata_v2_to_endianness(&array_metadata_v2.dtype)
            .map_err(ArrayMetadataV2ToV3ConversionError::InvalidEndianness)?,
    ) else {
        return Err(ArrayMetadataV2ToV3ConversionError::UnsupportedDataType(
            array_metadata_v2.dtype.clone(),
        ));
    };

    // Fill value
    let mut fill_value = fill_value_metadata_v2_to_v3(&array_metadata_v2.fill_value)
        .or_else(|| {
            // Support zarr-python encoded string arrays with a `null` fill value
            match data_type.name() {
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
        chunk_key_encoding::V2.to_string(),
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

/// Convert Zarr V2 data type metadata to Zarr V3.
///
/// # Errors
/// Returns a [`ArrayMetadataV2ToV3ConversionError`] if the data type is not supported.
pub fn data_type_metadata_v2_to_v3(
    data_type: &DataTypeMetadataV2,
    data_type_aliases_v2: &ExtensionAliasesDataTypeV2,
    data_type_aliases_v3: &ExtensionAliasesDataTypeV3,
) -> Result<MetadataV3, ArrayMetadataV2ToV3ConversionError> {
    match data_type {
        DataTypeMetadataV2::Simple(name) => {
            let identifier = data_type_aliases_v2.identifier(name);
            let name = data_type_aliases_v3.default_name(identifier).to_string();
            Ok(MetadataV3::new(name))
        }
        DataTypeMetadataV2::Structured(_) => Err(
            ArrayMetadataV2ToV3ConversionError::UnsupportedDataType(data_type.clone()),
        ),
    }
}

/// Convert Zarr V2 fill value metadata to Zarr V3.
///
/// Returns [`None`] for [`FillValueMetadataV2::Null`].
#[must_use]
pub fn fill_value_metadata_v2_to_v3(
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
