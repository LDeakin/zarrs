#![allow(missing_docs)]

use std::{borrow::Cow, sync::Arc};

use num::traits::{FromBytes, ToBytes};
use serde::{Deserialize, Serialize};
use zarrs::array::{
    ArrayBuilder, ArrayBytes, ArrayError, DataTypeSize, Element, ElementOwned, FillValueMetadataV3,
};
use zarrs_data_type::{
    DataType, DataTypeExtension, DataTypeExtensionBytesCodec, DataTypeExtensionBytesCodecError,
    DataTypeExtensionError, DataTypePlugin, FillValue, IncompatibleFillValueError,
    IncompatibleFillValueMetadataError,
};
use zarrs_metadata::{
    v3::{MetadataConfiguration, MetadataV3},
    Endianness,
};
use zarrs_plugin::{PluginCreateError, PluginMetadataInvalidError};
use zarrs_storage::store::MemoryStore;

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
struct CustomDataTypeFixedSizeElement {
    x: u64,
    y: f32,
}

type CustomDataTypeFixedSizeMetadata = CustomDataTypeFixedSizeElement;

type CustomDataTypeFixedSizeBytes = [u8; size_of::<u64>() + size_of::<f32>()];

impl ToBytes for CustomDataTypeFixedSizeElement {
    type Bytes = CustomDataTypeFixedSizeBytes;

    fn to_be_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 12];
        let (x, y) = bytes.split_at_mut(size_of::<u64>());
        x.copy_from_slice(&self.x.to_be_bytes());
        y.copy_from_slice(&self.y.to_be_bytes());
        bytes
    }

    fn to_le_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 12];
        let (x, y) = bytes.split_at_mut(size_of::<u64>());
        x.copy_from_slice(&self.x.to_le_bytes());
        y.copy_from_slice(&self.y.to_le_bytes());
        bytes
    }
}

impl FromBytes for CustomDataTypeFixedSizeElement {
    type Bytes = CustomDataTypeFixedSizeBytes;

    fn from_be_bytes(bytes: &Self::Bytes) -> Self {
        let (x, y) = bytes.split_at(size_of::<u64>());
        CustomDataTypeFixedSizeElement {
            x: u64::from_be_bytes(unsafe { x.try_into().unwrap_unchecked() }),
            y: f32::from_be_bytes(unsafe { y.try_into().unwrap_unchecked() }),
        }
    }

    fn from_le_bytes(bytes: &Self::Bytes) -> Self {
        let (x, y) = bytes.split_at(size_of::<u64>());
        CustomDataTypeFixedSizeElement {
            x: u64::from_le_bytes(unsafe { x.try_into().unwrap_unchecked() }),
            y: f32::from_le_bytes(unsafe { y.try_into().unwrap_unchecked() }),
        }
    }
}

impl Element for CustomDataTypeFixedSizeElement {
    fn validate_data_type(data_type: &DataType) -> Result<(), ArrayError> {
        (data_type == &DataType::Extension(Arc::new(CustomDataTypeFixedSize)))
            .then_some(())
            .ok_or(ArrayError::IncompatibleElementType)
    }

    fn into_array_bytes<'a>(
        data_type: &DataType,
        elements: &'a [Self],
    ) -> Result<zarrs::array::ArrayBytes<'a>, ArrayError> {
        Self::validate_data_type(data_type)?;
        let mut bytes: Vec<u8> =
            Vec::with_capacity(size_of::<CustomDataTypeFixedSizeBytes>() * elements.len());
        for element in elements {
            bytes.extend_from_slice(&element.to_ne_bytes());
        }
        Ok(ArrayBytes::Fixed(Cow::Owned(bytes)))
    }
}

impl ElementOwned for CustomDataTypeFixedSizeElement {
    fn from_array_bytes(
        data_type: &DataType,
        bytes: ArrayBytes<'_>,
    ) -> Result<Vec<Self>, ArrayError> {
        Self::validate_data_type(data_type)?;
        let bytes = bytes.into_fixed()?;
        let bytes_len = bytes.len();
        let mut elements =
            Vec::with_capacity(bytes_len / size_of::<CustomDataTypeFixedSizeBytes>());
        for bytes in bytes.chunks_exact(size_of::<CustomDataTypeFixedSizeBytes>()) {
            elements.push(CustomDataTypeFixedSizeElement::from_ne_bytes(unsafe {
                bytes.try_into().unwrap_unchecked()
            }))
        }
        Ok(elements)
    }
}

/// The data type for an array of [`CustomDataTypeFixedSizeElement`].
#[derive(Debug)]
struct CustomDataTypeFixedSize;

const CUSTOM_NAME: &'static str = "zarrs.test.CustomDataTypeFixedSize";

fn is_custom_dtype(name: &str) -> bool {
    name == CUSTOM_NAME
}

fn create_custom_dtype(metadata: &MetadataV3) -> Result<DataType, PluginCreateError> {
    if metadata.configuration_is_none_or_empty() {
        Ok(DataType::Extension(Arc::new(CustomDataTypeFixedSize)))
    } else {
        Err(PluginMetadataInvalidError::new(CUSTOM_NAME, "codec", metadata.clone()).into())
    }
}

inventory::submit! {
    DataTypePlugin::new(CUSTOM_NAME, is_custom_dtype, create_custom_dtype)
}

impl DataTypeExtension for CustomDataTypeFixedSize {
    fn name(&self) -> String {
        CUSTOM_NAME.to_string()
    }

    fn configuration(&self) -> MetadataConfiguration {
        MetadataConfiguration::default()
    }

    fn fill_value(
        &self,
        fill_value_metadata: &FillValueMetadataV3,
    ) -> Result<FillValue, IncompatibleFillValueMetadataError> {
        let custom_fill_value = match fill_value_metadata {
            FillValueMetadataV3::Unsupported(value) => serde_json::from_value::<
                CustomDataTypeFixedSizeMetadata,
            >(value.clone())
            .map_err(|_| {
                IncompatibleFillValueMetadataError::new(self.name(), fill_value_metadata.clone())
            })?,
            _ => Err(IncompatibleFillValueMetadataError::new(
                self.name(),
                fill_value_metadata.clone(),
            ))?,
        };
        Ok(FillValue::new(custom_fill_value.to_ne_bytes().to_vec()))
    }

    fn metadata_fill_value(
        &self,
        fill_value: &FillValue,
    ) -> Result<FillValueMetadataV3, IncompatibleFillValueError> {
        let fill_value_metadata = CustomDataTypeFixedSizeMetadata::from_ne_bytes(
            fill_value
                .as_ne_bytes()
                .try_into()
                .map_err(|_| IncompatibleFillValueError::new(self.name(), fill_value.clone()))?,
        );
        Ok(FillValueMetadataV3::Unsupported(
            serde_json::to_value(fill_value_metadata).unwrap(),
        ))
    }

    fn size(&self) -> zarrs::array::DataTypeSize {
        DataTypeSize::Fixed(size_of::<CustomDataTypeFixedSizeBytes>())
    }

    fn codec_bytes(&self) -> Result<&dyn DataTypeExtensionBytesCodec, DataTypeExtensionError> {
        Ok(self)
    }
}

impl DataTypeExtensionBytesCodec for CustomDataTypeFixedSize {
    fn encode<'a>(
        &self,
        bytes: std::borrow::Cow<'a, [u8]>,
        endianness: Option<zarrs_metadata::Endianness>,
    ) -> Result<std::borrow::Cow<'a, [u8]>, DataTypeExtensionBytesCodecError> {
        if let Some(endianness) = endianness {
            if endianness != Endianness::native() {
                let mut bytes = bytes.into_owned();
                for bytes in bytes.chunks_exact_mut(size_of::<CustomDataTypeFixedSizeBytes>()) {
                    let value = CustomDataTypeFixedSizeElement::from_ne_bytes(&unsafe {
                        bytes.try_into().unwrap_unchecked()
                    });
                    if endianness == Endianness::Little {
                        bytes.copy_from_slice(&value.to_le_bytes());
                    } else {
                        bytes.copy_from_slice(&value.to_be_bytes());
                    }
                }
                Ok(Cow::Owned(bytes))
            } else {
                Ok(bytes)
            }
        } else {
            Err(DataTypeExtensionBytesCodecError::EndiannessNotSpecified)
        }
    }

    fn decode<'a>(
        &self,
        bytes: std::borrow::Cow<'a, [u8]>,
        endianness: Option<zarrs_metadata::Endianness>,
    ) -> Result<std::borrow::Cow<'a, [u8]>, DataTypeExtensionBytesCodecError> {
        if let Some(endianness) = endianness {
            if endianness != Endianness::native() {
                let mut bytes = bytes.into_owned();
                for bytes in bytes.chunks_exact_mut(size_of::<u64>() + size_of::<f32>()) {
                    let value = if endianness == Endianness::Little {
                        CustomDataTypeFixedSizeElement::from_le_bytes(&unsafe {
                            bytes.try_into().unwrap_unchecked()
                        })
                    } else {
                        CustomDataTypeFixedSizeElement::from_be_bytes(&unsafe {
                            bytes.try_into().unwrap_unchecked()
                        })
                    };
                    bytes.copy_from_slice(&value.to_ne_bytes());
                }
                Ok(Cow::Owned(bytes))
            } else {
                Ok(bytes)
            }
        } else {
            Err(DataTypeExtensionBytesCodecError::EndiannessNotSpecified)
        }
    }
}

fn main() {
    let store = std::sync::Arc::new(MemoryStore::default());
    let array_path = "/array";
    let fill_value = CustomDataTypeFixedSizeElement { x: 1, y: 2.3 };
    let array = ArrayBuilder::new(
        vec![4, 1], // array shape
        DataType::Extension(Arc::new(CustomDataTypeFixedSize)),
        vec![2, 1].try_into().unwrap(), // regular chunk shape
        FillValue::new(fill_value.to_ne_bytes().to_vec()),
    )
    .array_to_array_codecs(vec![
        #[cfg(feature = "transpose")]
        Arc::new(zarrs::array::codec::TransposeCodec::new(
            zarrs::array::codec::array_to_array::transpose::TransposeOrder::new(&[1, 0]).unwrap(),
        )),
    ])
    .bytes_to_bytes_codecs(vec![
        #[cfg(feature = "gzip")]
        Arc::new(zarrs::array::codec::GzipCodec::new(5).unwrap()),
        #[cfg(feature = "crc32c")]
        Arc::new(zarrs::array::codec::Crc32cCodec::new()),
    ])
    // .storage_transformers(vec![].into())
    .build(store, array_path)
    .unwrap();
    println!(
        "{}",
        serde_json::to_string_pretty(array.metadata()).unwrap()
    );

    let data = [
        CustomDataTypeFixedSizeElement { x: 3, y: 4.5 },
        CustomDataTypeFixedSizeElement { x: 6, y: 7.8 },
    ];
    array.store_chunk_elements(&[0, 0], &data).unwrap();

    let data = array
        .retrieve_array_subset_elements::<CustomDataTypeFixedSizeElement>(&array.subset_all())
        .unwrap();

    assert_eq!(data[0], CustomDataTypeFixedSizeElement { x: 3, y: 4.5 });
    assert_eq!(data[1], CustomDataTypeFixedSizeElement { x: 6, y: 7.8 });
    assert_eq!(data[2], CustomDataTypeFixedSizeElement { x: 1, y: 2.3 });
    assert_eq!(data[3], CustomDataTypeFixedSizeElement { x: 1, y: 2.3 });

    println!("{data:#?}");
}

#[test]
fn custom_data_type_fixed_size() {
    main()
}
