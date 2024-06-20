use derive_more::Display;

/// The endianness of each element in an array, either `big` or `little`.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Display)]
pub enum Endianness {
    /// Little endian.
    Little,

    /// Big endian.
    Big,
}

impl Endianness {
    /// Return true if the endianness matches the endianness of the CPU.
    #[must_use]
    pub fn is_native(self) -> bool {
        self == NATIVE_ENDIAN
    }
}

/// The endianness of the CPU.
pub const NATIVE_ENDIAN: Endianness = if cfg!(target_endian = "big") {
    Endianness::Big
} else {
    Endianness::Little
};
