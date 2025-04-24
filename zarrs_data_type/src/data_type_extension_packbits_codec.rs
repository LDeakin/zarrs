/// Traits for a data type extension supporting the `packbits` codec.
pub trait DataTypeExtensionPackBitsCodec {
    /// The component size in bits.
    fn component_size_bits(&self) -> u64;

    /// The number of components.
    fn num_components(&self) -> u64;

    /// True if the components need sign extension.
    ///
    /// This should be set to `true` for signed integer types.
    fn sign_extension(&self) -> bool;
}
