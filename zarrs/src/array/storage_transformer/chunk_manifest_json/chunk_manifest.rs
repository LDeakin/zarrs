use std::{collections::HashMap, path::PathBuf};

use derive_more::derive::Deref;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct ChunkManifestValue {
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Eq, PartialEq, Debug, Deref)]
pub struct ChunkManifest(HashMap<String, ChunkManifestValue>);

impl<'de> serde::Deserialize<'de> for ChunkManifest {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let map = serde_json::Map::<String, serde_json::Value>::deserialize(d)?;
        let mut map_out = HashMap::with_capacity(map.len());
        for (key, v) in map {
            let value: ChunkManifestValue = serde_json::from_value(v)
                .map_err(|err| serde::de::Error::custom(err.to_string()))?;
            map_out.insert(key, value);
        }
        Ok(ChunkManifest(map_out))
    }
}
