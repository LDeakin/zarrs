//! `r*` raw bits data type. Variable size in bits given by *.

// TODO: Make this a standard part of DataType and don't lock behind a feature

use crate::{
    array::{data_type::DataTypePlugin, FillValue, FillValueMetadata},
    metadata::{ConfigurationInvalidError, Metadata},
    plugin::PluginCreateError,
};

use super::{DataTypeExtension, IncompatibleFillValueErrorMetadataError};

const IDENTIFIER: &str = "r*";

// Register the data type.
inventory::submit! {
    DataTypePlugin::new(IDENTIFIER, is_name_raw_bits, create_data_type_raw_bits)
}

fn is_name_raw_bits(name: &str) -> bool {
    name.starts_with('r') && name[1..].parse::<usize>().is_ok()
}

fn create_data_type_raw_bits(
    metadata: &Metadata,
) -> Result<Box<dyn DataTypeExtension>, PluginCreateError> {
    if metadata.configuration_is_none_or_empty() {
        let size_bits = metadata.name()[1..].parse::<usize>().unwrap(); // Safe because if is_name_raw_bits
        if size_bits % 8 == 0 {
            let data_type = RawBitsDataType(size_bits / 8);
            Ok(Box::new(data_type))
        } else {
            Err(PluginCreateError::Unsupported {
                name: metadata.name().to_string(),
            })
        }
    } else {
        Err(ConfigurationInvalidError::new(IDENTIFIER, metadata.configuration().cloned()).into())
    }
}

/// The `r*` raw bits data type.
/// Variable size in bits given by *.
///
/// Variable size is limited to be a multiple of 8.
#[derive(Clone, Debug)]
pub struct RawBitsDataType(usize);

impl DataTypeExtension for RawBitsDataType {
    fn identifier(&self) -> &'static str {
        IDENTIFIER
    }

    fn name(&self) -> String {
        format!("r{}", self.0 * 8)
    }

    fn size(&self) -> usize {
        self.0
    }

    fn metadata(&self) -> Metadata {
        Metadata::new(&self.name())
    }

    fn fill_value_from_metadata(
        &self,
        fill_value: &FillValueMetadata,
    ) -> Result<FillValue, IncompatibleFillValueErrorMetadataError> {
        match fill_value {
            FillValueMetadata::ByteArray(bytes) => Ok(FillValue::new(bytes.clone())),
            _ => Err(IncompatibleFillValueErrorMetadataError(
                self.name().to_string(),
                fill_value.clone(),
            )),
        }
    }

    fn metadata_fill_value(&self, fill_value: &FillValue) -> FillValueMetadata {
        assert_eq!(self.size(), fill_value.size());
        FillValueMetadata::ByteArray(fill_value.as_ne_bytes().to_vec())
    }
}
