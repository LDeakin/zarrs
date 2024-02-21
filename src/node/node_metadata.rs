use crate::{array::ArrayMetadata, group::GroupMetadata};

/// Node metadata ([`ArrayMetadata`] or [`GroupMetadata`]).
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum NodeMetadata {
    /// Array metadata.
    Array(ArrayMetadata),

    /// Group metadata.
    Group(GroupMetadata),
}
