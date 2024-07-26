//! Zarr array metadata.
//!
//! See <https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#array-metadata>.

pub use super::v3::ArrayMetadataV3;
use super::{
    v2::{
        array::{
            array_metadata_fill_value_v2_to_v3, data_type_metadata_v2_to_endianness,
            data_type_metadata_v2_to_v3_data_type, ArrayMetadataV2, ArrayMetadataV2DataType,
            ArrayMetadataV2Order, DataTypeMetadataV2InvalidEndiannessError, FillValueMetadataV2,
        },
        codec::{
            blosc::{codec_blosc_v2_numcodecs_to_v3, BloscCodecConfigurationNumcodecs},
            zfpy::{codec_zfpy_v2_numcodecs_to_v3, ZfpyCodecConfigurationNumcodecs},
        },
    },
    v3::codec::vlen_v2::VlenV2CodecConfigurationV1,
};
use thiserror::Error;

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

use crate::{
    array::{
        chunk_grid::RegularChunkGridConfiguration,
        chunk_key_encoding::V2ChunkKeyEncodingConfiguration, codec::BytesCodecConfigurationV1,
        DataType, FillValueMetadata,
    },
    metadata::{
        v3::{
            codec::transpose::{TransposeCodecConfigurationV1, TransposeOrder},
            MetadataV3,
        },
        AdditionalFields,
    },
};

/// A wrapper to handle various versions of Zarr array metadata.
#[derive(Deserialize, Serialize, Clone, PartialEq, Debug, Display, From)]
#[serde(untagged)]
pub enum ArrayMetadata {
    /// Zarr Version 3.0.
    V3(ArrayMetadataV3),
    /// Zarr Version 2.0.
    V2(ArrayMetadataV2),
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
    UnsupportedFillValue(DataType, FillValueMetadataV2),
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
        crate::array::chunk_grid::regular::IDENTIFIER,
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
                ArrayMetadataV2DataType::Simple(dtype) => dtype.clone(),
                ArrayMetadataV2DataType::Structured(dtype) => {
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
                data_type.clone(),
                array_metadata_v2.fill_value.clone(),
            )
        })?;
    if data_type == crate::array::DataType::Bool {
        // Map a 0/1 scalar fill value to a bool
        if let Some(fill_value_uint) = fill_value.try_as_uint::<u64>() {
            if fill_value_uint == 0 {
                fill_value = FillValueMetadata::Bool(false);
            } else if fill_value_uint == 1 {
                fill_value = FillValueMetadata::Bool(true);
            } else {
                return Err(ArrayMetadataV2ToV3ConversionError::UnsupportedFillValue(
                    data_type,
                    array_metadata_v2.fill_value.clone(),
                ));
            }
        }
    }

    let mut codecs: Vec<MetadataV3> = vec![];

    // Array-to-array codecs
    if array_metadata_v2.order == ArrayMetadataV2Order::F {
        let transpose_metadata = MetadataV3::new_with_serializable_configuration(
            super::v3::codec::transpose::IDENTIFIER,
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
                        super::v3::codec::vlen_v2::IDENTIFIER,
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
            super::v2::codec::zfpy::IDENTIFIER => {
                has_array_to_bytes = true;
                let zfpy_v2_metadata = serde_json::from_value::<ZfpyCodecConfigurationNumcodecs>(
                    serde_json::to_value(compressor.configuration())?,
                )?;
                let configuration = codec_zfpy_v2_numcodecs_to_v3(&zfpy_v2_metadata)?;
                let zfp_v3_metadata = MetadataV3::new_with_serializable_configuration(
                    super::v3::codec::zfp::IDENTIFIER,
                    &configuration,
                )?;
                codecs.push(zfp_v3_metadata);
            }
            super::v3::codec::pcodec::IDENTIFIER => {
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
            super::v3::codec::bytes::IDENTIFIER,
            &BytesCodecConfigurationV1 { endian: endianness },
        )?;
        codecs.push(bytes_metadata);
    }

    // Compressor (bytes to bytes codec)
    if let Some(compressor) = &array_metadata_v2.compressor {
        match compressor.id() {
            super::v2::codec::zfpy::IDENTIFIER | super::v3::codec::pcodec::IDENTIFIER => {
                // already handled above
            }
            super::v3::codec::blosc::IDENTIFIER => {
                let blosc = serde_json::from_value::<BloscCodecConfigurationNumcodecs>(
                    serde_json::to_value(compressor.configuration())?,
                )?;
                let configuration = codec_blosc_v2_numcodecs_to_v3(&blosc, &data_type);
                codecs.push(MetadataV3::new_with_serializable_configuration(
                    super::v3::codec::blosc::IDENTIFIER,
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
        crate::array::chunk_key_encoding::v2::IDENTIFIER,
        &V2ChunkKeyEncodingConfiguration {
            separator: array_metadata_v2.dimension_separator,
        },
    )?;

    let attributes = array_metadata_v2.attributes.clone();

    Ok(ArrayMetadataV3::new(
        shape,
        data_type.metadata(),
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

impl TryFrom<&str> for ArrayMetadata {
    type Error = serde_json::Error;
    fn try_from(metadata_json: &str) -> Result<Self, Self::Error> {
        serde_json::from_str::<Self>(metadata_json)
    }
}
