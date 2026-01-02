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

//! Performance metrics for ORC file operations.
//!
//! This module provides metrics for monitoring and analyzing ORC file reading
//! performance, including I/O statistics, pruning effectiveness, and timing.
//!
//! # Metric Categories
//!
//! - **I/O Metrics**: Track bytes scanned and I/O requests
//! - **Metadata Metrics**: Track metadata loading time
//! - **Stripe Pruning**: Track stripe-level filtering effectiveness
//! - **Predicate Evaluation**: Track predicate pushdown statistics
//!
//! # Example
//!
//! ```ignore
//! use datafusion_datasource_orc::metrics::OrcFileMetrics;
//! use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
//!
//! let metrics_set = ExecutionPlanMetricsSet::new();
//! let metrics = OrcFileMetrics::new(0, "example.orc", &metrics_set);
//!
//! // Record bytes scanned
//! metrics.bytes_scanned.add(1024);
//!
//! // Record metadata load time
//! let timer = metrics.metadata_load_time.timer();
//! // ... load metadata ...
//! timer.done();
//! ```

use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricType, PruningMetrics, Time,
};

/// Metrics for ORC file operations.
///
/// Tracks performance statistics for reading ORC files, including I/O,
/// metadata loading, stripe pruning, and predicate evaluation.
#[derive(Debug, Clone)]
pub struct OrcFileMetrics {
    // =========================================================================
    // I/O Metrics
    // =========================================================================
    /// Total number of bytes scanned from the file.
    ///
    /// This includes all data read from the object store, including metadata,
    /// stripe data, and index data.
    pub bytes_scanned: Count,

    /// Total file size in bytes (for calculating scan efficiency).
    pub file_size: Count,

    /// Number of I/O requests made to the object store.
    pub io_requests: Count,

    // =========================================================================
    // Metadata Metrics
    // =========================================================================
    /// Time spent reading and parsing ORC file metadata (footer, postscript).
    pub metadata_load_time: Time,

    // =========================================================================
    // Stripe Pruning Metrics
    // =========================================================================
    /// Number of stripes pruned or matched by statistics.
    ///
    /// This uses `PruningMetrics` which tracks both pruned and matched counts.
    /// - `pruned`: Stripes skipped due to statistics not matching the predicate
    /// - `matched`: Stripes that were read because they might contain matching rows
    pub stripes_pruned_statistics: PruningMetrics,

    /// Time spent evaluating stripe-level statistics for pruning.
    pub statistics_eval_time: Time,

    // =========================================================================
    // Predicate Evaluation Metrics
    // =========================================================================
    /// Number of times predicate evaluation encountered errors.
    ///
    /// This can happen when statistics are malformed or when the predicate
    /// cannot be evaluated against the available metadata.
    pub predicate_evaluation_errors: Count,

    /// Number of rows filtered out by predicates pushed into the ORC scan.
    pub pushdown_rows_pruned: Count,

    /// Number of rows that passed predicates pushed into the ORC scan.
    pub pushdown_rows_matched: Count,

    // =========================================================================
    // Decode Metrics
    // =========================================================================
    /// Time spent decoding ORC data into Arrow arrays.
    pub decode_time: Time,

    /// Total number of rows decoded from the file.
    pub rows_decoded: Count,

    /// Number of RecordBatches produced.
    pub batches_produced: Count,
}

impl OrcFileMetrics {
    /// Create new ORC file metrics.
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition index for this scan
    /// * `filename` - The name of the ORC file being scanned
    /// * `metrics` - The metrics set to register metrics with
    pub fn new(partition: usize, filename: &str, metrics: &ExecutionPlanMetricsSet) -> Self {
        // -----------------------
        // Summary level metrics (user-visible)
        // -----------------------
        let bytes_scanned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .counter("bytes_scanned", partition);

        let file_size = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .counter("file_size", partition);

        let metadata_load_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .subset_time("metadata_load_time", partition);

        let stripes_pruned_statistics = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .pruning_metrics("stripes_pruned_statistics", partition);

        // -----------------------
        // Dev level metrics (for debugging/optimization)
        // -----------------------
        let io_requests = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("io_requests", partition);

        let statistics_eval_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .subset_time("statistics_eval_time", partition);

        let predicate_evaluation_errors = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("predicate_evaluation_errors", partition);

        let pushdown_rows_pruned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("pushdown_rows_pruned", partition);

        let pushdown_rows_matched = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("pushdown_rows_matched", partition);

        let decode_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .subset_time("decode_time", partition);

        let rows_decoded = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("rows_decoded", partition);

        let batches_produced = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("batches_produced", partition);

        Self {
            bytes_scanned,
            file_size,
            io_requests,
            metadata_load_time,
            stripes_pruned_statistics,
            statistics_eval_time,
            predicate_evaluation_errors,
            pushdown_rows_pruned,
            pushdown_rows_matched,
            decode_time,
            rows_decoded,
            batches_produced,
        }
    }

    /// Calculate the scan efficiency ratio.
    ///
    /// Returns the ratio of bytes scanned to total file size.
    /// - 1.0 = entire file was read
    /// - 0.5 = half the file was read (due to projection/filtering)
    /// - 0.0 = no data read (all stripes pruned)
    ///
    /// Returns `None` if file size is zero.
    pub fn scan_efficiency(&self) -> Option<f64> {
        let file_size = self.file_size.value();
        if file_size == 0 {
            return None;
        }
        Some(self.bytes_scanned.value() as f64 / file_size as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orc_file_metrics_creation() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = OrcFileMetrics::new(0, "test.orc", &metrics_set);

        // Verify metrics are initialized to zero
        assert_eq!(metrics.bytes_scanned.value(), 0);
        assert_eq!(metrics.io_requests.value(), 0);
        assert_eq!(metrics.rows_decoded.value(), 0);
        assert_eq!(metrics.batches_produced.value(), 0);
    }

    #[test]
    fn test_orc_file_metrics_recording() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = OrcFileMetrics::new(0, "test.orc", &metrics_set);

        // Record some metrics
        metrics.bytes_scanned.add(1024);
        metrics.io_requests.add(5);
        metrics.rows_decoded.add(100);
        metrics.batches_produced.add(2);

        assert_eq!(metrics.bytes_scanned.value(), 1024);
        assert_eq!(metrics.io_requests.value(), 5);
        assert_eq!(metrics.rows_decoded.value(), 100);
        assert_eq!(metrics.batches_produced.value(), 2);
    }

    #[test]
    fn test_orc_file_metrics_scan_efficiency() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = OrcFileMetrics::new(0, "test.orc", &metrics_set);

        // Set file size and bytes scanned
        metrics.file_size.add(1024);
        metrics.bytes_scanned.add(512);

        // Scan efficiency should be 0.5 (512/1024)
        let efficiency = metrics.scan_efficiency().unwrap();
        assert!((efficiency - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_orc_file_metrics_scan_efficiency_zero_file() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = OrcFileMetrics::new(0, "test.orc", &metrics_set);

        // File size is zero
        assert!(metrics.scan_efficiency().is_none());
    }

    #[test]
    fn test_orc_file_metrics_stripe_pruning() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = OrcFileMetrics::new(0, "test.orc", &metrics_set);

        // Record stripe pruning
        metrics.stripes_pruned_statistics.add_pruned(3);
        metrics.stripes_pruned_statistics.add_matched(2);

        assert_eq!(metrics.stripes_pruned_statistics.pruned(), 3);
        assert_eq!(metrics.stripes_pruned_statistics.matched(), 2);
    }

    #[test]
    fn test_orc_file_metrics_predicate_evaluation() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = OrcFileMetrics::new(0, "test.orc", &metrics_set);

        // Record predicate evaluation results
        metrics.pushdown_rows_pruned.add(50);
        metrics.pushdown_rows_matched.add(100);
        metrics.predicate_evaluation_errors.add(1);

        assert_eq!(metrics.pushdown_rows_pruned.value(), 50);
        assert_eq!(metrics.pushdown_rows_matched.value(), 100);
        assert_eq!(metrics.predicate_evaluation_errors.value(), 1);
    }

    #[test]
    fn test_orc_file_metrics_timing() {
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = OrcFileMetrics::new(0, "test.orc", &metrics_set);

        // Test timer (basic functionality)
        let timer = metrics.metadata_load_time.timer();
        // Simulate some work
        std::thread::sleep(std::time::Duration::from_millis(1));
        timer.done();

        // Time should be recorded (greater than 0)
        assert!(metrics.metadata_load_time.value() > 0);
    }
}
