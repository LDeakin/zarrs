use std::collections::HashMap;

use regex::Regex;

use super::{ExtensionAliases, ExtensionTypeDataType};

use crate::v3::array::data_type::{
    BOOL, BYTES, COMPLEX128, COMPLEX64, FLOAT16, FLOAT32, FLOAT64, INT16, INT32, INT64, INT8,
    STRING, UINT16, UINT32, UINT64, UINT8,
};
use crate::version::{ZarrVersion2, ZarrVersion3};

/// Zarr V3 *data type* extension aliases.
pub type ExtensionAliasesDataTypeV3 = ExtensionAliases<ZarrVersion3, ExtensionTypeDataType>;

/// Zarr V2 *data type* extension aliases.
pub type ExtensionAliasesDataTypeV2 = ExtensionAliases<ZarrVersion2, ExtensionTypeDataType>;

impl Default for ExtensionAliasesDataTypeV3 {
    #[rustfmt::skip]
    fn default() -> Self {
        Self::new(
            // The default serialised `name`s
            HashMap::from([]),
            // `name` aliases (string match)
            HashMap::from([
                ("binary".into(), BYTES), // ZEP0007 uses binary, zarr-python uses bytes
            ]),
            // `name` aliases (regex match)
            vec![],
        )
    }
}

impl Default for ExtensionAliasesDataTypeV2 {
    #[rustfmt::skip]
    fn default() -> Self {
        Self::new(
            // The default serialised `name`s
            HashMap::from([]),
            // `name` aliases (string match)
            HashMap::from([
                ("|b1".into(), BOOL),
                ("|i1".into(), INT8),
                ("<i2".into(), INT16),
                (">i2".into(), INT16),
                ("<i4".into(), INT32),
                (">i4".into(), INT32),
                ("<i8".into(), INT64),
                (">i8".into(), INT64),
                ("|u1".into(), UINT8),
                ("<u2".into(), UINT16),
                (">u2".into(), UINT16),
                ("<u4".into(), UINT32),
                (">u4".into(), UINT32),
                ("<u8".into(), UINT64),
                (">u8".into(), UINT64),
                ("<f2".into(), FLOAT16),
                (">f2".into(), FLOAT16),
                ("<f4".into(), FLOAT32),
                (">f4".into(), FLOAT32),
                ("<f8".into(), FLOAT64),
                (">f8".into(), FLOAT64),
                ("<c8".into(), COMPLEX64),
                (">c8".into(), COMPLEX64),
                ("<c16".into(), COMPLEX128),
                (">c16".into(), COMPLEX128),
                ("|O".into(), STRING),
                ("|VX".into(), BYTES),
                // ("|mX".into(), "timedelta"),
                // ("|MX".into(), "datetime"),
                // ("|SX".into(), "fixedcharstring"),
                // ("|UX".into(), "fixedunicodestring"),
            ]),
            // `name` aliases (regex match)
            vec![
                (Regex::new(r"^\|V\d+$").expect("valid"), BYTES),
                // (Regex::new(r"^\|m\d+$").expect("valid"), "timedelta"),
                // (Regex::new(r"^\|M\d+$").expect("valid"), "datetime"),
                // (Regex::new(r"^\|S\d+$").expect("valid"), "fixedcharstring"),
                // (Regex::new(r"^\|U\d+$").expect("valid"), "fixedunicodestring"),
            ],
        )
    }
}
