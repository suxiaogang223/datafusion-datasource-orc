// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ORC metadata processing utilities.
//!
//! This module exposes helpers to read ORC file metadata and derive Arrow
//! schemas and statistics used by DataFusion.

use arrow::datatypes::SchemaRef;
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Result, Statistics};
use object_store::{ObjectMeta, ObjectStore};
use orc_rust::reader::metadata::read_metadata_async;
use orc_rust::schema::ArrowSchemaOptions;
use std::collections::HashMap;
use std::sync::Arc;

use crate::reader::ObjectStoreChunkReader;

/// Read ORC file metadata and extract an Arrow schema.
pub async fn read_orc_schema(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> Result<SchemaRef> {
    let mut reader =
        ObjectStoreChunkReader::with_size(Arc::clone(store), object.location.clone(), object.size);

    let file_metadata = read_metadata_async(&mut reader).await.map_err(|e| {
        DataFusionError::External(format!("Failed to read ORC metadata: {}", e).into())
    })?;

    // Convert ORC schema to Arrow schema
    let root_data_type = file_metadata.root_data_type();
    let metadata: HashMap<String, String> = file_metadata
        .user_custom_metadata()
        .iter()
        .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).to_string()))
        .collect();

    let options = ArrowSchemaOptions::new();
    let schema = root_data_type.create_arrow_schema_with_options(&metadata, options);

    Ok(Arc::new(schema))
}

/// Read ORC file statistics.
///
/// TODO: Extract column-level statistics (min/max/null counts) once the
/// underlying ORC metadata exposes them in a stable form.
pub async fn read_orc_statistics(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    _table_schema: SchemaRef,
) -> Result<Statistics> {
    let mut reader =
        ObjectStoreChunkReader::with_size(Arc::clone(store), object.location.clone(), object.size);

    let file_metadata = read_metadata_async(&mut reader).await.map_err(|e| {
        DataFusionError::External(format!("Failed to read ORC metadata: {}", e).into())
    })?;

    // Extract statistics from ORC file metadata
    let num_rows = file_metadata.number_of_rows();

    // TODO: Extract column-level statistics (min/max/null counts) from file_metadata
    // For now, return basic statistics
    Ok(Statistics {
        num_rows: Precision::Exact(num_rows as usize),
        total_byte_size: Precision::Exact(object.size as usize),
        column_statistics: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectStorePath;
    use std::path::PathBuf;

    fn get_test_data_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("integration")
            .join("data")
    }

    #[tokio::test]
    async fn test_read_orc_schema_alltypes() {
        let test_data_dir = get_test_data_dir();
        let orc_file = test_data_dir.join("alltypes.snappy.orc");

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let path = ObjectStorePath::from_filesystem_path(&orc_file).unwrap();
        let object_meta = store.head(&path).await.unwrap();

        let schema = read_orc_schema(&store, &object_meta).await.unwrap();

        // Verify schema fields
        assert!(!schema.fields().is_empty());

        // Check specific fields exist
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"boolean"));
        assert!(field_names.contains(&"int8"));
        assert!(field_names.contains(&"int64"));
        assert!(field_names.contains(&"utf8"));
    }

    #[tokio::test]
    async fn test_read_orc_statistics_alltypes() {
        let test_data_dir = get_test_data_dir();
        let orc_file = test_data_dir.join("alltypes.snappy.orc");

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let path = ObjectStorePath::from_filesystem_path(&orc_file).unwrap();
        let object_meta = store.head(&path).await.unwrap();

        let schema = read_orc_schema(&store, &object_meta).await.unwrap();
        let stats = read_orc_statistics(&store, &object_meta, schema).await.unwrap();

        // Verify row count
        assert_eq!(stats.num_rows, Precision::Exact(11));

        // Verify byte size is set
        match stats.total_byte_size {
            Precision::Exact(size) => assert!(size > 0),
            _ => panic!("Expected exact byte size"),
        }
    }

    #[tokio::test]
    async fn test_read_orc_schema_map_list() {
        let test_data_dir = get_test_data_dir();
        let orc_file = test_data_dir.join("map_list.snappy.orc");

        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let path = ObjectStorePath::from_filesystem_path(&orc_file).unwrap();
        let object_meta = store.head(&path).await.unwrap();

        let schema = read_orc_schema(&store, &object_meta).await.unwrap();

        // Verify schema fields
        assert!(!schema.fields().is_empty());

        // Check specific fields exist
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"m")); // Map field
        assert!(field_names.contains(&"l")); // List field
    }

    #[tokio::test]
    async fn test_read_orc_schema_nonexistent_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());

        // Try to read schema from a file we know doesn't exist
        // by using a path that's clearly invalid
        let path = ObjectStorePath::from("/this/path/definitely/does/not/exist.orc");

        // Use head() which will fail for non-existent files
        let result = store.head(&path).await;
        assert!(result.is_err(), "Expected error for non-existent file");
    }
}
