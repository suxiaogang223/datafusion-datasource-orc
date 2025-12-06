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

//! [`OrcFormat`]: ORC [`FileFormat`] abstractions

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::{GetExt, Result};
use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};

/// Factory struct used to create [`OrcFormat`]
#[derive(Debug, Default)]
pub struct OrcFormatFactory;

impl OrcFormatFactory {
    /// Creates an instance of [`OrcFormatFactory`]
    pub fn new() -> Self {
        Self
    }
}

impl FileFormatFactory for OrcFormatFactory {
    fn create(
        &self,
        _state: &dyn datafusion_session::Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(OrcFormat))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(OrcFormat)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for OrcFormatFactory {
    fn get_ext(&self) -> String {
        "orc".to_string()
    }
}

/// The Apache ORC `FileFormat` implementation
#[derive(Debug, Default)]
pub struct OrcFormat;

impl OrcFormat {
    /// Construct a new Format with default options
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FileFormat for OrcFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "orc".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &datafusion_datasource::file_compression_type::FileCompressionType,
    ) -> Result<String> {
        // ORC files have built-in compression, so the extension is always "orc"
        Ok("orc".to_string())
    }

    fn compression_type(
        &self,
    ) -> Option<datafusion_datasource::file_compression_type::FileCompressionType> {
        // ORC files have built-in compression support
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn datafusion_session::Session,
        _store: &Arc<dyn object_store::ObjectStore>,
        _objects: &[object_store::ObjectMeta],
    ) -> Result<arrow::datatypes::SchemaRef> {
        todo!("Schema inference not yet implemented")
    }

    async fn infer_stats(
        &self,
        _state: &dyn datafusion_session::Session,
        _store: &Arc<dyn object_store::ObjectStore>,
        _table_schema: arrow::datatypes::SchemaRef,
        _object: &object_store::ObjectMeta,
    ) -> Result<datafusion_common::Statistics> {
        todo!("Statistics inference not yet implemented")
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn datafusion_session::Session,
        _conf: datafusion_datasource::file_scan_config::FileScanConfig,
    ) -> Result<Arc<dyn datafusion_physical_plan::ExecutionPlan>> {
        todo!("Physical plan creation not yet implemented")
    }

    fn file_source(&self) -> Arc<dyn datafusion_datasource::file::FileSource> {
        todo!("File source creation not yet implemented")
    }
}
