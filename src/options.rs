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

//! ORC-specific configuration types.
//!
//! This module mirrors the configuration style used by DataFusion's Parquet
//! datasource. It currently focuses on read-path options; write options are
//! reserved for future work.

use std::collections::HashMap;

use datafusion_common::{DataFusionError, Result};

/// Options that control how ORC files are read.
#[derive(Clone, Debug)]
pub struct OrcReadOptions {
    /// Optional batch size override used when a scan does not specify one.
    pub batch_size: Option<usize>,
    /// Enable converting predicates into ORC stripe-level filters.
    pub pushdown_predicate: bool,
    /// Optional metadata size hint for ORC footer reads.
    pub metadata_size_hint: Option<usize>,
}

impl Default for OrcReadOptions {
    fn default() -> Self {
        Self {
            batch_size: None,
            pushdown_predicate: true,
            metadata_size_hint: None,
        }
    }
}

impl OrcReadOptions {
    /// Set a default batch size for ORC scans.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Enable or disable predicate pushdown into the ORC reader.
    pub fn with_pushdown_predicate(mut self, pushdown_predicate: bool) -> Self {
        self.pushdown_predicate = pushdown_predicate;
        self
    }

    /// Provide a hint for how many bytes to read when fetching ORC metadata.
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.metadata_size_hint = Some(metadata_size_hint);
        self
    }
}

/// Top-level ORC format options.
#[derive(Clone, Debug, Default)]
pub struct OrcFormatOptions {
    /// Read-path configuration.
    pub read: OrcReadOptions,
    // TODO: Add write options when ORC writer support lands.
}

impl OrcFormatOptions {
    /// Apply a set of key-value options from a DataFusion format options map.
    pub fn apply_format_options(&mut self, format_options: &HashMap<String, String>) -> Result<()> {
        for (key, value) in format_options {
            match key.as_str() {
                "orc.batch_size" => {
                    self.read.batch_size = Some(parse_usize_option(key, value)?);
                }
                "orc.pushdown_predicate" => {
                    self.read.pushdown_predicate = parse_bool_option(key, value)?;
                }
                "orc.metadata_size_hint" => {
                    self.read.metadata_size_hint = Some(parse_usize_option(key, value)?);
                }
                _ => {
                    // TODO: Validate unknown keys once ORC options are formalized in DataFusion.
                }
            }
        }
        Ok(())
    }
}

fn parse_bool_option(key: &str, value: &str) -> Result<bool> {
    value.parse::<bool>().map_err(|_| {
        DataFusionError::Configuration(format!(
            "Invalid value for {key}: {value}. Expected true or false."
        ))
    })
}

fn parse_usize_option(key: &str, value: &str) -> Result<usize> {
    value.parse::<usize>().map_err(|_| {
        DataFusionError::Configuration(format!(
            "Invalid value for {key}: {value}. Expected a positive integer."
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orc_read_options_default() {
        let options = OrcReadOptions::default();
        assert_eq!(options.batch_size, None);
        assert!(options.pushdown_predicate);
        assert_eq!(options.metadata_size_hint, None);
    }

    #[test]
    fn test_orc_read_options_builder() {
        let options = OrcReadOptions::default()
            .with_batch_size(4096)
            .with_pushdown_predicate(false)
            .with_metadata_size_hint(1024);

        assert_eq!(options.batch_size, Some(4096));
        assert!(!options.pushdown_predicate);
        assert_eq!(options.metadata_size_hint, Some(1024));
    }

    #[test]
    fn test_orc_format_options_default() {
        let options = OrcFormatOptions::default();
        assert_eq!(options.read.batch_size, None);
        assert!(options.read.pushdown_predicate);
    }

    #[test]
    fn test_apply_format_options_batch_size() {
        let mut options = OrcFormatOptions::default();
        let mut format_opts = HashMap::new();
        format_opts.insert("orc.batch_size".to_string(), "8192".to_string());

        options.apply_format_options(&format_opts).unwrap();
        assert_eq!(options.read.batch_size, Some(8192));
    }

    #[test]
    fn test_apply_format_options_pushdown_predicate() {
        let mut options = OrcFormatOptions::default();
        let mut format_opts = HashMap::new();
        format_opts.insert("orc.pushdown_predicate".to_string(), "false".to_string());

        options.apply_format_options(&format_opts).unwrap();
        assert!(!options.read.pushdown_predicate);
    }

    #[test]
    fn test_apply_format_options_metadata_size_hint() {
        let mut options = OrcFormatOptions::default();
        let mut format_opts = HashMap::new();
        format_opts.insert("orc.metadata_size_hint".to_string(), "1048576".to_string());

        options.apply_format_options(&format_opts).unwrap();
        assert_eq!(options.read.metadata_size_hint, Some(1048576));
    }

    #[test]
    fn test_apply_format_options_invalid_batch_size() {
        let mut options = OrcFormatOptions::default();
        let mut format_opts = HashMap::new();
        format_opts.insert("orc.batch_size".to_string(), "not_a_number".to_string());

        let result = options.apply_format_options(&format_opts);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid value for orc.batch_size"));
    }

    #[test]
    fn test_apply_format_options_invalid_bool() {
        let mut options = OrcFormatOptions::default();
        let mut format_opts = HashMap::new();
        format_opts.insert("orc.pushdown_predicate".to_string(), "maybe".to_string());

        let result = options.apply_format_options(&format_opts);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Invalid value for orc.pushdown_predicate"));
    }

    #[test]
    fn test_apply_format_options_multiple() {
        let mut options = OrcFormatOptions::default();
        let mut format_opts = HashMap::new();
        format_opts.insert("orc.batch_size".to_string(), "16384".to_string());
        format_opts.insert("orc.pushdown_predicate".to_string(), "true".to_string());
        format_opts.insert("orc.metadata_size_hint".to_string(), "2097152".to_string());

        options.apply_format_options(&format_opts).unwrap();
        assert_eq!(options.read.batch_size, Some(16384));
        assert!(options.read.pushdown_predicate);
        assert_eq!(options.read.metadata_size_hint, Some(2097152));
    }

    #[test]
    fn test_apply_format_options_unknown_key() {
        let mut options = OrcFormatOptions::default();
        let mut format_opts = HashMap::new();
        format_opts.insert("orc.unknown_option".to_string(), "value".to_string());

        // Unknown options should be silently ignored for now
        let result = options.apply_format_options(&format_opts);
        assert!(result.is_ok());
    }
}
