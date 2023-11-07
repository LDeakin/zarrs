use derive_more::Display;

/// The decoded representation of `bytes`.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Display)]
pub enum BytesRepresentation {
    /// The output size is fixed.
    #[display(fmt = "fixed size: {_0}")]
    FixedSize(u64),
    /// The output size is bounded.
    #[display(fmt = "bounded size: {_0}")]
    BoundedSize(u64),
    /// The output size is unbounded/indeterminate.
    #[display(fmt = "unbounded size")]
    UnboundedSize,
}

impl BytesRepresentation {
    /// Return the fixed or bounded size of the bytes representations, or [`None`] if the size is unbounded.
    #[must_use]
    pub fn size(&self) -> Option<u64> {
        match self {
            Self::FixedSize(size) | Self::BoundedSize(size) => Some(*size),
            Self::UnboundedSize => None,
        }
    }
}
