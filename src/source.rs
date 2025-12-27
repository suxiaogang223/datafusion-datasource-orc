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

//! ORC [`FileSource`] implementation for DataFusion scans.
//!
//! `OrcSource` wires DataFusion's [`FileScanConfig`] to an ORC-specific
//! [`FileOpener`] and manages scan-level options such as projections, limits,
//! and predicate pushdown.
//!
//! [`FileScanConfig`]: datafusion_datasource::file_scan_config::FileScanConfig
//! [`FileOpener`]: datafusion_datasource::file_stream::FileOpener
//! [`FileSource`]: datafusion_datasource::file::FileSource

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::Statistics;
use datafusion_datasource::as_file_source;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::TableSchema;
use datafusion_physical_expr::conjunction;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_plan::filter_pushdown::{
    FilterPushdownPropagation, PushedDown,
};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::DisplayFormatType;
use object_store::ObjectStore;
use orc_rust::predicate::Predicate as OrcPredicate;

use crate::options::OrcReadOptions;
use crate::opener::OrcOpener;
use crate::predicate::convert_physical_expr_to_predicate;

const DEFAULT_BATCH_SIZE: usize = 8192;

/// Execution plan for reading one or more ORC files.
///
/// Supports projection, limit pushdown, and stripe-level predicate pushdown.
/// TODO: Add schema adapter support and column statistics pruning.
#[derive(Clone)]
pub struct OrcSource {
    /// Table schema
    table_schema: TableSchema,
    /// Execution plan metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate filter pushed down from DataFusion
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Converted orc-rust predicate (cached for efficiency)
    orc_predicate: Option<OrcPredicate>,
    /// Read-path options for ORC scans
    read_options: OrcReadOptions,
    /// Statistics projected to the scan schema, if available
    projected_statistics: Option<Statistics>,
}

impl Debug for OrcSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrcSource")
            .field("table_schema", &self.table_schema)
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl OrcSource {
    /// Create a new OrcSource
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        Self {
            table_schema: table_schema.into(),
            metrics: ExecutionPlanMetricsSet::new(),
            predicate: None,
            orc_predicate: None,
            read_options: OrcReadOptions::default(),
            projected_statistics: None,
        }
    }

    /// Set read options for ORC scans.
    pub fn with_read_options(mut self, read_options: OrcReadOptions) -> Self {
        self.read_options = read_options;
        if let Some(predicate) = self.predicate.clone() {
            self.set_predicate(predicate);
        }
        self
    }

    /// Return the current read options.
    pub fn read_options(&self) -> &OrcReadOptions {
        &self.read_options
    }

    /// Create a new OrcSource with a predicate filter
    ///
    /// The predicate will be converted to an orc-rust Predicate and used
    /// for stripe-level filtering during file reads.
    pub fn with_predicate(&self, predicate: Arc<dyn PhysicalExpr>) -> Self {
        let mut source = self.clone();
        source.set_predicate(predicate);
        source
    }

    /// Get the orc-rust predicate (if conversion was successful)
    pub fn orc_predicate(&self) -> Option<&OrcPredicate> {
        self.orc_predicate.as_ref()
    }

    fn set_predicate(&mut self, predicate: Arc<dyn PhysicalExpr>) {
        self.predicate = Some(Arc::clone(&predicate));
        if self.read_options.pushdown_predicate {
            let file_schema = self.table_schema.file_schema();
            self.orc_predicate =
                convert_physical_expr_to_predicate(&predicate, file_schema.as_ref());
        } else {
            self.orc_predicate = None;
        }
    }
}

/// Allows easy conversion from OrcSource to Arc<dyn FileSource>
impl From<OrcSource> for Arc<dyn FileSource> {
    fn from(source: OrcSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for OrcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        // Extract projection indices from the file scan config
        let file_schema = base_config.file_schema();
        let projection: Arc<[usize]> = base_config
            .file_column_projection_indices()
            .map(|indices| indices.into())
            .unwrap_or_else(|| (0..file_schema.fields().len()).collect::<Vec<_>>().into());

        // Get batch size from config or use default
        let batch_size = base_config
            .batch_size
            .or(self.read_options.batch_size)
            .unwrap_or(DEFAULT_BATCH_SIZE);

        // Get limit from config
        let limit = base_config.limit;

        // Get projected file schema (without partition columns)
        let logical_file_schema = base_config.projected_file_schema();

        // Get partition fields
        let partition_fields = base_config.table_partition_cols().clone();

        // Get metrics
        let metrics = self.metrics.clone();

        // Clone the orc predicate for the opener
        let orc_predicate = self.orc_predicate.clone();

        Arc::new(OrcOpener::new(
            partition,
            projection,
            batch_size,
            limit,
            logical_file_schema,
            partition_fields,
            metrics,
            object_store,
            orc_predicate,
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.read_options = source.read_options.with_batch_size(batch_size);
        Arc::new(source)
    }

    fn with_schema(&self, schema: TableSchema) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.table_schema = schema;
        Arc::new(source)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_statistics(&self, statistics: datafusion_common::Statistics) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.projected_statistics = Some(statistics);
        Arc::new(source)
    }

    fn statistics(&self) -> datafusion_common::Result<datafusion_common::Statistics> {
        if let Some(statistics) = self.projected_statistics.clone() {
            if self.filter().is_some() {
                Ok(statistics.to_inexact())
            } else {
                Ok(statistics)
            }
        } else {
            Ok(datafusion_common::Statistics::new_unknown(
                self.table_schema.table_schema().as_ref(),
            ))
        }
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "orc"
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                if let Some(predicate) = self.filter() {
                    write!(f, ", predicate={predicate}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                if let Some(predicate) = self.filter() {
                    writeln!(f, "predicate={}", fmt_sql(predicate.as_ref()))?;
                }
                Ok(())
            }
        }
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<
        FilterPushdownPropagation<Arc<dyn FileSource>>,
    > {
        let file_schema = self.table_schema.file_schema();
        let total_filters = filters.len();
        let mut supported = Vec::new();

        for filter in filters {
            if convert_physical_expr_to_predicate(&filter, file_schema.as_ref()).is_some()
            {
                supported.push(filter);
            }
        }

        if supported.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; total_filters],
            ));
        }

        let mut source = self.clone();
        // ORC predicates are currently used for stripe-level pruning only. We
        // still report `No` so DataFusion applies the full filter above.
        source.set_predicate(conjunction(supported));
        let source = Arc::new(source);

        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; total_filters],
            )
            .with_updated_node(source),
        )
    }

    /// Returns the filter expression that will be applied during the file scan.
    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.predicate.clone()
    }
}
