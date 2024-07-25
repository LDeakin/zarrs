use std::sync::Arc;

use crate::{metadata::v3::AdditionalFields, node::NodePath, storage::StorageTransformerChain};

use super::{
    chunk_key_encoding::{ChunkKeyEncoding, DefaultChunkKeyEncoding},
    codec::{
        array_to_bytes::vlen::VlenCodec, ArrayToArrayCodecTraits, ArrayToBytesCodecTraits,
        BytesCodec, BytesToBytesCodecTraits,
    },
    data_type::IncompatibleFillValueError,
    Array, ArrayCreateError, ArrayMetadata, ArrayMetadataV3, ArrayShape, ChunkGrid,
    ChunkKeySeparator, CodecChain, DataType, DimensionName, FillValue,
};

/// An [`Array`] builder.
///
/// The array builder is initialised from an array shape, data type, chunk grid, and fill value.
///  - The only codec enabled by default is `bytes` (with native endian encoding), so the output is uncompressed.
///  - The default chunk key encoding is `default` with the `/` chunk key separator.
///  - Attributes, storage transformers, and dimension names are empty.
///  - Codecs are configured to use multiple threads where possible.
///
/// Use the methods in the array builder to change the configuration away from these defaults, and then build the array at a path of some storage with [`ArrayBuilder::build`].
/// Note that [`build`](ArrayBuilder::build) does not modify the store; the array metadata has to be explicitly written with [`Array::store_metadata`].
///
/// For example:
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # use std::sync::Arc;
/// use zarrs::array::{ArrayBuilder, DataType, FillValue, ZARR_NAN_F32};
/// # let store = Arc::new(zarrs::storage::store::MemoryStore::new());
/// let mut array = ArrayBuilder::new(
///     vec![8, 8], // array shape
///     DataType::Float32,
///     vec![4, 4].try_into()?, // regular chunk shape (elements must be non-zero)
///     FillValue::from(ZARR_NAN_F32),
/// )
/// .bytes_to_bytes_codecs(vec![
///     #[cfg(feature = "gzip")]
///     Box::new(zarrs::array::codec::GzipCodec::new(5)?),
/// ])
/// .dimension_names(["y", "x"].into())
/// .build(store.clone(), "/group/array")?;
/// array.store_metadata()?; // write metadata to the store
///
/// // array.store_chunk(...)
/// // array.store_array_subset(...)
///
/// array.set_shape(vec![16, 16]); // revise the shape if needed
/// array.store_metadata()?; // update stored metadata
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ArrayBuilder {
    /// Array shape.
    pub shape: ArrayShape,
    /// Data type.
    pub data_type: DataType,
    /// Chunk grid.
    pub chunk_grid: ChunkGrid,
    /// Chunk key encoding.
    pub chunk_key_encoding: ChunkKeyEncoding,
    /// Fill value.
    pub fill_value: FillValue,
    /// Array to array codecs.
    pub array_to_array_codecs: Vec<Box<dyn ArrayToArrayCodecTraits>>,
    /// Array to bytes codec.
    pub array_to_bytes_codec: Box<dyn ArrayToBytesCodecTraits>,
    /// Bytes to bytes codecs.
    pub bytes_to_bytes_codecs: Vec<Box<dyn BytesToBytesCodecTraits>>,
    /// Storage transformer chain.
    pub storage_transformers: StorageTransformerChain,
    /// Attributes.
    pub attributes: serde_json::Map<String, serde_json::Value>,
    /// Dimension names.
    pub dimension_names: Option<Vec<DimensionName>>,
    /// Additional fields.
    pub additional_fields: AdditionalFields,
}

impl ArrayBuilder {
    /// Create a new array builder for an array at `path`.
    ///
    /// The length of the array shape must match the dimensionality of the intended array, but it can be all zeros on initialisation.
    /// The shape of the [`Array`] can be be updated as required.
    #[must_use]
    pub fn new(
        shape: ArrayShape,
        data_type: DataType,
        chunk_grid: ChunkGrid,
        fill_value: FillValue,
    ) -> Self {
        let is_fixed_size = data_type.fixed_size().is_some();
        Self {
            shape,
            data_type,
            chunk_grid,
            chunk_key_encoding: ChunkKeyEncoding::new(DefaultChunkKeyEncoding::default()),
            fill_value,
            array_to_array_codecs: Vec::default(),
            array_to_bytes_codec: if is_fixed_size {
                Box::<BytesCodec>::default()
            } else {
                Box::<VlenCodec>::default()
                // Box::<VlenV2Codec>::default()
            },
            bytes_to_bytes_codecs: Vec::default(),
            attributes: serde_json::Map::default(),
            storage_transformers: StorageTransformerChain::default(),
            dimension_names: None,
            additional_fields: AdditionalFields::default(),
        }
    }

    /// Create a new builder copying the configuration of an existing array.
    #[must_use]
    pub fn from_array<T: ?Sized>(array: &Array<T>) -> Self {
        let mut builder = Self::new(
            array.shape().to_vec(),
            array.data_type().clone(),
            array.chunk_grid().clone(),
            array.fill_value().clone(),
        );
        builder
            .additional_fields(array.additional_fields().clone())
            .attributes(array.attributes().clone())
            .chunk_key_encoding(array.chunk_key_encoding().clone())
            .dimension_names(array.dimension_names().clone())
            .array_to_array_codecs(array.codecs().array_to_array_codecs().to_vec())
            .array_to_bytes_codec(array.codecs().array_to_bytes_codec().clone())
            .bytes_to_bytes_codecs(array.codecs().bytes_to_bytes_codecs().to_vec())
            .storage_transformers(array.storage_transformers().clone());
        builder
    }

    /// Set the shape.
    pub fn shape(&mut self, shape: ArrayShape) -> &mut Self {
        self.shape = shape;
        self
    }

    /// Set the data type.
    pub fn data_type(&mut self, data_type: DataType) -> &mut Self {
        self.data_type = data_type;
        self
    }

    /// Set the chunk grid.
    pub fn chunk_grid(&mut self, chunk_grid: ChunkGrid) -> &mut Self {
        self.chunk_grid = chunk_grid;
        self
    }

    /// Set the fill value.
    pub fn fill_value(&mut self, fill_value: FillValue) -> &mut Self {
        self.fill_value = fill_value;
        self
    }

    /// Set the chunk key encoding.
    ///
    /// If left unmodified, the array will use `default` chunk key encoding with the `/` chunk key separator.
    pub fn chunk_key_encoding(&mut self, chunk_key_encoding: ChunkKeyEncoding) -> &mut Self {
        self.chunk_key_encoding = chunk_key_encoding;
        self
    }

    /// Set the chunk key encoding to default with `separator`.
    ///
    /// If left unmodified, the array will use `default` chunk key encoding with the `/` chunk key separator.
    pub fn chunk_key_encoding_default_separator(
        &mut self,
        separator: ChunkKeySeparator,
    ) -> &mut Self {
        self.chunk_key_encoding = ChunkKeyEncoding::new(DefaultChunkKeyEncoding::new(separator));
        self
    }

    /// Set the array to array codecs.
    ///
    /// If left unmodified, the array will have no array to array codecs.
    pub fn array_to_array_codecs(
        &mut self,
        array_to_array_codecs: Vec<Box<dyn ArrayToArrayCodecTraits>>,
    ) -> &mut Self {
        self.array_to_array_codecs = array_to_array_codecs;
        self
    }

    /// Set the array to bytes codec.
    ///
    /// If left unmodified, the array will default to using the `bytes` codec with native endian encoding.
    pub fn array_to_bytes_codec(
        &mut self,
        array_to_bytes_codec: Box<dyn ArrayToBytesCodecTraits>,
    ) -> &mut Self {
        self.array_to_bytes_codec = array_to_bytes_codec;
        self
    }

    /// Set the bytes to bytes codecs.
    ///
    /// If left unmodified, the array will have no bytes to bytes codecs.
    pub fn bytes_to_bytes_codecs(
        &mut self,
        bytes_to_bytes_codecs: Vec<Box<dyn BytesToBytesCodecTraits>>,
    ) -> &mut Self {
        self.bytes_to_bytes_codecs = bytes_to_bytes_codecs;
        self
    }

    /// Set the user defined attributes.
    ///
    /// If left unmodified, the user defined attributes of the array will be empty.
    pub fn attributes(
        &mut self,
        attributes: serde_json::Map<String, serde_json::Value>,
    ) -> &mut Self {
        self.attributes = attributes;
        self
    }

    /// Set the additional fields.
    ///
    /// Set additional fields not defined in the Zarr specification.
    /// Use this cautiously. In general, store user defined attributes using [`ArrayBuilder::attributes`].
    ///
    /// Note that array metadata must not contain any additional fields, unless they are annotated with `"must_understand": false`.
    /// `zarrs` will error when opening an array with additional fields without this annotation.
    pub fn additional_fields(&mut self, additional_fields: AdditionalFields) -> &mut Self {
        self.additional_fields = additional_fields;
        self
    }

    /// Set the dimension names.
    ///
    /// If left unmodified, all dimension names are "unnamed".
    pub fn dimension_names<I, D>(&mut self, dimension_names: Option<I>) -> &mut Self
    where
        I: IntoIterator<Item = D>,
        D: Into<DimensionName>,
    {
        if let Some(dimension_names) = dimension_names {
            self.dimension_names = Some(
                dimension_names
                    .into_iter()
                    .map(Into::<DimensionName>::into)
                    .collect(),
            );
        } else {
            self.dimension_names = None;
        }
        self
    }

    /// Set the storage transformers.
    ///
    /// If left unmodified, there are no storage transformers.
    pub fn storage_transformers(
        &mut self,
        storage_transformers: StorageTransformerChain,
    ) -> &mut Self {
        self.storage_transformers = storage_transformers;
        self
    }

    /// Build into an [`Array`].
    ///
    /// # Errors
    ///
    /// Returns [`ArrayCreateError`] if there is an error creating the array.
    /// This can be due to a storage error, an invalid path, or a problem with array configuration.
    pub fn build<TStorage: ?Sized>(
        &self,
        storage: Arc<TStorage>,
        path: &str,
    ) -> Result<Array<TStorage>, ArrayCreateError> {
        let path: NodePath = path.try_into()?;
        if self.chunk_grid.dimensionality() != self.shape.len() {
            return Err(ArrayCreateError::InvalidChunkGridDimensionality(
                self.chunk_grid.dimensionality(),
                self.shape.len(),
            ));
        }
        if let Some(dimension_names) = &self.dimension_names {
            if dimension_names.len() != self.shape.len() {
                return Err(ArrayCreateError::InvalidDimensionNames(
                    dimension_names.len(),
                    self.shape.len(),
                ));
            }
        }

        if let Some(data_type_size) = self.data_type.fixed_size() {
            if data_type_size != self.fill_value.size() {
                return Err(IncompatibleFillValueError::new(
                    self.data_type.name(),
                    self.fill_value.clone(),
                )
                .into());
            }
        }

        let codec_chain = CodecChain::new(
            self.array_to_array_codecs.clone(),
            self.array_to_bytes_codec.clone(),
            self.bytes_to_bytes_codecs.clone(),
        );

        let array_metadata = ArrayMetadata::V3(ArrayMetadataV3::new(
            self.shape.clone(),
            self.data_type.metadata(),
            self.chunk_grid.create_metadata(),
            self.chunk_key_encoding.create_metadata(),
            self.data_type.metadata_fill_value(&self.fill_value),
            codec_chain.create_metadatas(),
            self.attributes.clone(),
            self.storage_transformers.create_metadatas(),
            self.dimension_names.clone(),
            self.additional_fields.clone(),
        ));

        Ok(Array {
            storage,
            path,
            // shape: self.shape.clone(),
            data_type: self.data_type.clone(),
            chunk_grid: self.chunk_grid.clone(),
            chunk_key_encoding: self.chunk_key_encoding.clone(),
            fill_value: self.fill_value.clone(),
            codecs: CodecChain::new(
                self.array_to_array_codecs.clone(),
                self.array_to_bytes_codec.clone(),
                self.bytes_to_bytes_codecs.clone(),
            ),
            storage_transformers: self.storage_transformers.clone(),
            // attributes: self.attributes.clone(),
            dimension_names: self.dimension_names.clone(),
            // additional_fields: self.additional_fields.clone(),
            metadata: array_metadata,
        })
    }

    /// Build into an [`Arc<Array>`].
    ///
    /// # Errors
    ///
    /// Returns [`ArrayCreateError`] if there is an error creating the array.
    /// This can be due to a storage error, an invalid path, or a problem with array configuration.
    pub fn build_arc<TStorage: ?Sized>(
        &self,
        storage: Arc<TStorage>,
        path: &str,
    ) -> Result<Arc<Array<TStorage>>, ArrayCreateError> {
        Ok(Arc::new(self.build(storage, path)?))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{chunk_grid::RegularChunkGrid, chunk_key_encoding::V2ChunkKeyEncoding},
        storage::{storage_transformer::UsageLogStorageTransformer, store::MemoryStore},
    };

    use super::*;

    #[test]
    fn array_builder() {
        let mut builder = ArrayBuilder::new(
            vec![8, 8],
            DataType::Int8,
            vec![2, 2].try_into().unwrap(),
            FillValue::from(0i8),
        );

        // Coverage
        builder.shape(vec![8, 8]);
        builder.data_type(DataType::Int8);
        // builder.chunk_grid(vec![2, 2].try_into().unwrap());
        builder.chunk_grid(ChunkGrid::new(RegularChunkGrid::new(
            vec![2, 2].try_into().unwrap(),
        )));
        builder.fill_value(FillValue::from(0i8));

        builder.dimension_names(["y", "x"].into());

        let mut attributes = serde_json::Map::new();
        attributes.insert("key".to_string(), "value".into());
        builder.attributes(attributes.clone());

        let mut additional_fields = AdditionalFields::new();
        let additional_field = serde_json::Map::new();
        additional_fields.insert("key".to_string(), additional_field.into());
        builder.additional_fields(additional_fields.clone());

        builder.chunk_key_encoding(V2ChunkKeyEncoding::new_dot().into());
        builder.chunk_key_encoding_default_separator(ChunkKeySeparator::Dot); // overrides previous
        let log_writer = Arc::new(std::sync::Mutex::new(std::io::stdout()));
        let usage_log = Arc::new(UsageLogStorageTransformer::new(log_writer, || {
            chrono::Utc::now().format("[%T%.3f] ").to_string()
        }));

        builder.storage_transformers(StorageTransformerChain::new(vec![usage_log]));

        let storage = Arc::new(MemoryStore::new());
        println!("{:?}", builder.build(storage.clone(), "/"));
        let array = builder.build(storage, "/").unwrap();
        assert_eq!(array.shape(), &[8, 8]);
        assert_eq!(array.data_type(), &DataType::Int8);
        assert_eq!(array.chunk_grid_shape(), Some(vec![4, 4]));
        assert_eq!(array.fill_value(), &FillValue::from(0i8));
        assert_eq!(array.dimension_names(), &Some(vec!["y".into(), "x".into()]));
        assert_eq!(array.attributes(), &attributes);
        assert_eq!(array.additional_fields(), &additional_fields);

        let builder2 = array.builder();
        assert_eq!(builder.shape, builder2.shape);
        assert_eq!(builder.data_type, builder2.data_type);
        assert_eq!(builder.fill_value, builder2.fill_value);
        assert_eq!(builder.attributes, builder2.attributes);
        assert_eq!(builder.dimension_names, builder2.dimension_names);
        assert_eq!(builder.additional_fields, builder2.additional_fields);
    }

    #[test]
    fn array_builder_invalid() {
        let storage = Arc::new(MemoryStore::new());
        // Invalid chunk shape
        let builder = ArrayBuilder::new(
            vec![8, 8],
            DataType::Int8,
            vec![2, 2, 2].try_into().unwrap(),
            FillValue::from(0i8),
        );
        assert!(builder.build(storage.clone(), "/").is_err());
        // Invalid fill value
        let builder = ArrayBuilder::new(
            vec![8, 8],
            DataType::Int8,
            vec![2, 2].try_into().unwrap(),
            FillValue::from(0i16),
        );
        assert!(builder.build(storage.clone(), "/").is_err());
        // Invalid dimension names
        let mut builder = ArrayBuilder::new(
            vec![8, 8],
            DataType::Int8,
            vec![2, 2].try_into().unwrap(),
            FillValue::from(0i8),
        );
        builder.dimension_names(["z", "y", "x"].into());
        assert!(builder.build(storage.clone(), "/").is_err());
    }
}
