//! `bfloat16` brain floating point data type: sign bit, 5 bits exponent, 10 bits mantissa.

use half::bf16;

use crate::{
    array::{
        data_type::DataTypePlugin,
        fill_value_metadata::{FillValueFloat, FillValueFloatStringNonFinite, FillValueMetadata},
        FillValue,
    },
    metadata::{ConfigurationInvalidError, Metadata},
    plugin::PluginCreateError,
};

use super::{DataTypeExtension, IncompatibleFillValueErrorMetadataError};

const IDENTIFIER: &str = "bfloat16";

// Register the data type.
inventory::submit! {
    DataTypePlugin::new(IDENTIFIER, is_name_bfloat16, create_data_type_bfloat16)
}

fn is_name_bfloat16(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

fn create_data_type_bfloat16(
    metadata: &Metadata,
) -> Result<Box<dyn DataTypeExtension>, PluginCreateError> {
    if metadata.configuration_is_none_or_empty() {
        let data_type = Bfloat16DataType;
        Ok(Box::new(data_type))
    } else {
        Err(ConfigurationInvalidError::new(IDENTIFIER, metadata.configuration().cloned()).into())
    }
}

/// The `bfloat16` data type.
/// Brain floating point: sign bit, 5 bits exponent, 10 bits mantissa.
#[derive(Clone, Debug)]
pub struct Bfloat16DataType;

impl DataTypeExtension for Bfloat16DataType {
    fn identifier(&self) -> &'static str {
        IDENTIFIER
    }

    fn name(&self) -> String {
        IDENTIFIER.to_string()
    }

    fn size(&self) -> usize {
        2
    }

    fn metadata(&self) -> Metadata {
        Metadata::new(IDENTIFIER)
    }

    fn fill_value_from_metadata(
        &self,
        fill_value: &FillValueMetadata,
    ) -> Result<FillValue, IncompatibleFillValueErrorMetadataError> {
        let float = match fill_value {
            FillValueMetadata::Float(float) => {
                use FillValueFloat as F;
                match float {
                    F::Float(float) => Some(bf16::from_f64(*float)),
                    F::HexString(hex_string) => {
                        let bytes = hex_string.as_bytes();
                        if bytes.len() == core::mem::size_of::<bf16>() {
                            Some(bf16::from_be_bytes(bytes.try_into().unwrap()))
                        } else {
                            None
                        }
                    }
                    F::NonFinite(nonfinite) => {
                        use FillValueFloatStringNonFinite as NF;
                        Some(match nonfinite {
                            NF::PosInfinity => bf16::INFINITY,
                            NF::NegInfinity => bf16::NEG_INFINITY,
                            NF::NaN => bf16::NAN,
                        })
                    }
                }
            }
            _ => None,
        };
        Ok(float
            .ok_or(IncompatibleFillValueErrorMetadataError(
                self.name().to_string(),
                fill_value.clone(),
            ))?
            .into())
    }

    fn metadata_fill_value(&self, fill_value: &FillValue) -> FillValueMetadata {
        assert_eq!(self.size(), fill_value.size());
        let fill_value = bf16::from_ne_bytes(fill_value.as_ne_bytes().try_into().unwrap());
        FillValueMetadata::Float(bfloat16_to_fill_value_float(fill_value))
    }
}

fn bfloat16_to_fill_value_float(f: bf16) -> FillValueFloat {
    if f.is_infinite() && f.is_sign_positive() {
        FillValueFloatStringNonFinite::PosInfinity.into()
    } else if f.is_infinite() && f.is_sign_negative() {
        FillValueFloatStringNonFinite::NegInfinity.into()
    } else if f.is_nan() {
        FillValueFloatStringNonFinite::NaN.into()
    } else {
        f64::from(f).into()
    }
}
