/// Traits for a data type extension supporting the `packbits` codec.
pub trait DataTypeExtensionPackBitsCodec {
    /// Return the size in bits of the data type.
    fn size_bits(&self) -> u8;
}
