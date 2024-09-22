use std::sync::Arc;

use crate::metadata::v3::MetadataV3;

use crate::node::NodePath;
use crate::{
    array::storage_transformer::StorageTransformerPlugin,
    plugin::{PluginCreateError, PluginMetadataInvalidError},
};

use super::StorageTransformer;

mod chunk_manifest_json_configuration;
pub use chunk_manifest_json_configuration::ChunkManifestJsonConfiguration;

mod chunk_manifest;
pub use chunk_manifest::ChunkManifest;

mod chunk_manifest_storage_transformer;
pub use chunk_manifest_storage_transformer::ChunkManifestJsonStorageTransformer;

pub const IDENTIFIER: &str = "chunk-manifest-json";

// Register the storage plugin.
inventory::submit! {
    StorageTransformerPlugin::new(IDENTIFIER, is_name_chunk_manifest_json, create_storage_transformer_chunk_manifest_json)
}

fn is_name_chunk_manifest_json(name: &str) -> bool {
    name.eq(IDENTIFIER)
}

pub(crate) fn create_storage_transformer_chunk_manifest_json(
    metadata: &MetadataV3,
    path: &NodePath,
) -> Result<StorageTransformer, PluginCreateError> {
    let configuration: ChunkManifestJsonConfiguration =
        metadata.to_configuration().map_err(|_| {
            PluginMetadataInvalidError::new(IDENTIFIER, "storage_transformer", metadata.clone())
        })?;
    let chunk_manifest_json: StorageTransformer = Arc::new(
        ChunkManifestJsonStorageTransformer::new(configuration, path),
    );
    Ok(chunk_manifest_json)
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::BufReader};

    use super::ChunkManifest;

    #[test]
    fn chunk_manifest() {
        let file = File::open("tests/data/virtualizarr/virtualizarr.zarr/data/manifest.json")
            .expect("Failed to open manifest file");
        let reader = BufReader::new(file);
        let chunk_manifest: Result<ChunkManifest, _> = serde_json::from_reader(reader);
        assert!(chunk_manifest.is_ok());
    }
}
