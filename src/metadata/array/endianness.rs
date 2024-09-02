use derive_more::derive::Display;
use serde::{Deserialize, Serialize};

/// Endianness. Either `big` or `little`.
#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Debug, Display)]
#[serde(rename_all = "lowercase")]
pub enum Endianness {
    /// Little endian.
    Little,

    /// Big endian.
    Big,
}

/// The endianness of the CPU.
const NATIVE_ENDIAN: Endianness = if cfg!(target_endian = "big") {
    Endianness::Big
} else {
    Endianness::Little
};

impl Endianness {
    /// Returns the native endianness of the CPU.
    #[must_use]
    pub fn native() -> Endianness {
        NATIVE_ENDIAN
    }

    /// Return true if the endianness matches the endianness of the CPU.
    #[must_use]
    pub fn is_native(self) -> bool {
        self == NATIVE_ENDIAN
    }
}
