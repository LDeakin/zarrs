use zarrs_metadata::v3::MetadataV3;
use zarrs_plugin::{Plugin, PluginCreateError};

use crate::DataType;

/// A data type plugin.
#[derive(derive_more::Deref)]
pub struct DataTypePlugin(Plugin<DataType, MetadataV3>);
inventory::collect!(DataTypePlugin);

impl DataTypePlugin {
    /// Create a new [`DataTypePlugin`].
    pub const fn new(
        identifier: &'static str,
        match_name_fn: fn(name: &str) -> bool,
        create_fn: fn(metadata: &MetadataV3) -> Result<DataType, PluginCreateError>,
    ) -> Self {
        Self(Plugin::new(identifier, match_name_fn, create_fn))
    }
}
