use derive_more::{Display, From};

/// The decoded representation of `bytes`.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Display, From)]
pub enum BytesRepresentation {
    /// The output size is known.
    #[display(fmt = "bytes: {_0}")]
    KnownSize(u64),
    /// The output size may vary.
    #[display(fmt = "bytes: variable")]
    VariableSize,
}
