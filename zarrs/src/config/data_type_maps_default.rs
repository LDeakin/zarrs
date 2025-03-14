use std::collections::HashMap;

use zarrs_metadata::ExtensionMapsDataType;

/// The default extension data type mapping.
#[rustfmt::skip]
pub(crate) fn data_type_maps_default() -> ExtensionMapsDataType {
    ExtensionMapsDataType::new(
        // Default data type names
        HashMap::from([
        ]),
        // Zarr v3 aliases
        HashMap::from([
            ("binary".into(), "bytes"),
        ]),
        // Zarr v2 aliases
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
            ("|VX".into(), "bytes")
            // ("|mX".into(), "timedelta"),
            // ("|MX".into(), "datetime"),
            // ("|SX".into(), "fixedcharstring"),
            // ("|UX".into(), "fixedunicodestring"),
        ]),
        // Zarr V3 regex replacements
        vec![],
        // Zarr V3 regex replacements
        vec![],
    )
}
