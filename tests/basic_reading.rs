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

//! Integration tests for basic ORC file reading functionality

use datafusion::prelude::*;
use datafusion_datasource::file_format::FileFormat;
use datafusion_orc_extension::OrcFormat;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::path::PathBuf;
use std::sync::Arc;

/// Format schema to a normalized string for comparison
fn format_schema(schema: &arrow::datatypes::Schema) -> String {
    let mut fields: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            format!(
                "{}: {}",
                f.name(),
                format_data_type(f.data_type())
            )
        })
        .collect();
    fields.sort();
    fields.join(", ")
}

/// Format data type to string representation
fn format_data_type(dt: &arrow::datatypes::DataType) -> String {
    match dt {
        arrow::datatypes::DataType::Boolean => "Boolean".to_string(),
        arrow::datatypes::DataType::Int8 => "Int8".to_string(),
        arrow::datatypes::DataType::Int16 => "Int16".to_string(),
        arrow::datatypes::DataType::Int32 => "Int32".to_string(),
        arrow::datatypes::DataType::Int64 => "Int64".to_string(),
        arrow::datatypes::DataType::Float32 => "Float32".to_string(),
        arrow::datatypes::DataType::Float64 => "Float64".to_string(),
        arrow::datatypes::DataType::Utf8 => "Utf8".to_string(),
        arrow::datatypes::DataType::Binary => "Binary".to_string(),
        arrow::datatypes::DataType::Date32 => "Date32".to_string(),
        arrow::datatypes::DataType::Decimal128(precision, scale) => {
            format!("Decimal128({}, {})", precision, scale)
        }
        arrow::datatypes::DataType::List(field) => {
            format!(
                "List({}{})",
                format_data_type(field.data_type()),
                if field.is_nullable() { ", nullable" } else { "" }
            )
        }
        arrow::datatypes::DataType::Map(field, _) => {
            match field.data_type() {
                arrow::datatypes::DataType::Struct(struct_fields) => {
                    let entries: Vec<String> = struct_fields
                        .iter()
                        .map(|f| {
                            format!(
                                "{}: {}{}",
                                f.name(),
                                format_data_type(f.data_type()),
                                if f.is_nullable() { " (nullable)" } else { "" }
                            )
                        })
                        .collect();
                    format!("Map(Struct({}))", entries.join(", "))
                }
                _ => format!("Map({})", format_data_type(field.data_type())),
            }
        }
        _ => format!("{:?}", dt),
    }
}

/// Helper function to create a test ObjectStore
fn create_test_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

/// Helper function to get test data directory
fn get_test_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integration")
        .join("data")
}

#[tokio::test]
async fn test_schema_inference_alltypes() {
    let test_data_dir = get_test_data_dir();
    let orc_file = test_data_dir.join("alltypes.snappy.orc");

    let object_store = create_test_object_store();
    let format = OrcFormat::new();

    // Get file metadata - convert PathBuf to string first
    let file_path: object_store::path::Path = orc_file.to_string_lossy().as_ref().into();
    let file_meta = object_store
        .head(&file_path)
        .await
        .expect("Failed to get file metadata");

    // Create SessionContext and get SessionState
    let ctx = SessionContext::new();
    let session_state = ctx.state();

    // Infer schema
    let schema = format
        .infer_schema(&session_state, &object_store, &[file_meta])
        .await
        .expect("Failed to infer schema");

    // Verify schema is not empty
    assert!(!schema.fields().is_empty(), "Expected non-empty schema");

    // Format schema to string and compare with expected result
    let schema_str = format_schema(&schema);
    let expected = "binary: Binary, boolean: Boolean, date32: Date32, decimal: Decimal128(15, 5), float32: Float32, float64: Float64, int16: Int16, int32: Int32, int64: Int64, int8: Int8, utf8: Utf8";
    
    assert_eq!(
        schema_str, expected,
        "\nSchema mismatch!\nExpected: {}\nGot:      {}",
        expected, schema_str
    );

    println!("Inferred schema: {}", schema_str);
}

#[tokio::test]
async fn test_schema_inference_map_list() {
    let test_data_dir = get_test_data_dir();
    let orc_file = test_data_dir.join("map_list.snappy.orc");

    let object_store = create_test_object_store();
    let format = OrcFormat::new();

    // Get file metadata - convert PathBuf to string first
    let file_path: object_store::path::Path = orc_file.to_string_lossy().as_ref().into();
    let file_meta = object_store
        .head(&file_path)
        .await
        .expect("Failed to get file metadata");

    // Create SessionContext and get SessionState
    let ctx = SessionContext::new();
    let session_state = ctx.state();

    // Infer schema
    let schema = format
        .infer_schema(&session_state, &object_store, &[file_meta])
        .await
        .expect("Failed to infer schema");

    // Verify schema is not empty
    assert!(!schema.fields().is_empty(), "Expected non-empty schema");

    // Format schema to string and compare with expected result
    let schema_str = format_schema(&schema);
    let expected = "id: Int64, l: List(Utf8, nullable), m: Map(Struct(keys: Utf8, values: Utf8 (nullable))), s: Utf8";
    
    assert_eq!(
        schema_str, expected,
        "\nSchema mismatch!\nExpected: {}\nGot:      {}",
        expected, schema_str
    );

    println!("Inferred schema: {}", schema_str);
}
