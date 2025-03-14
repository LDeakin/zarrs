use std::collections::HashMap;

use regex::Regex;

use crate::{ExtensionAliases, ExtensionTypeDataType, ZarrVersion2, ZarrVersion3};

/// Zarr V3 data type extension aliases.
pub type ExtensionAliasesDataTypeV3 = ExtensionAliases<ZarrVersion3, ExtensionTypeDataType>;

/// Zarr V2 data type extension aliases.
pub type ExtensionAliasesDataTypeV2 = ExtensionAliases<ZarrVersion2, ExtensionTypeDataType>;

impl Default for ExtensionAliasesDataTypeV3 {
    #[rustfmt::skip]
    fn default() -> Self {
        Self::new(
            // The default serialised `name`s
            HashMap::from([]),
            // `name` aliases (string match)
            HashMap::from([
                ("binary".into(), "bytes"), // ZEP0007 uses binary, zarr-python uses bytes
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
                ("|b1".into(), "bool"),
                ("|i1".into(), "int8"),
                ("<i2".into(), "int16"),
                (">i2".into(), "int16"),
                ("<i4".into(), "int32"),
                (">i4".into(), "int32"),
                ("<i8".into(), "int64"),
                (">i8".into(), "int64"),
                ("|u1".into(), "uint8"),
                ("<u2".into(), "uint16"),
                (">u2".into(), "uint16"),
                ("<u4".into(), "uint32"),
                (">u4".into(), "uint32"),
                ("<u8".into(), "uint64"),
                (">u8".into(), "uint64"),
                ("<f2".into(), "float16"),
                (">f2".into(), "float16"),
                ("<f4".into(), "float32"),
                (">f4".into(), "float32"),
                ("<f8".into(), "float64"),
                (">f8".into(), "float64"),
                ("<c8".into(), "complex64"),
                (">c8".into(), "complex64"),
                ("<c16".into(), "complex128"),
                (">c16".into(), "complex128"),
                ("|O".into(), "string"),
                ("|VX".into(), "bytes"),
                // ("|mX".into(), "timedelta"),
                // ("|MX".into(), "datetime"),
                // ("|SX".into(), "fixedcharstring"),
                // ("|UX".into(), "fixedunicodestring"),
            ]),
            // `name` aliases (regex match)
            vec![
                (Regex::new(r"^\|V\d+$").expect("valid"), "bytes"),
                // (Regex::new(r"^\|m\d+$").expect("valid"), "timedelta"),
                // (Regex::new(r"^\|M\d+$").expect("valid"), "datetime"),
                // (Regex::new(r"^\|S\d+$").expect("valid"), "fixedcharstring"),
                // (Regex::new(r"^\|U\d+$").expect("valid"), "fixedunicodestring"),
            ],
        )
    }
}
