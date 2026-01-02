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

//! ORC datasource for Apache DataFusion.
//!
//! This crate provides DataFusion [`FileFormat`] and [`FileSource`] implementations
//! backed by [`orc-rust`]. It integrates with DataFusion's listing tables and
//! reads ORC files asynchronously via [`object_store`].
//!
//! # Features
//!
//! - **Schema Inference**: Automatically infer table schema from ORC files
//! - **Statistics Extraction**: Extract file statistics (row count, file size)
//! - **Projection Pushdown**: Read only the columns needed by the query
//! - **Limit Pushdown**: Stop reading after the required number of rows
//! - **Predicate Pushdown**: Filter data at stripe level using ORC row indexes
//! - **Multi-file Support**: Read from multiple ORC files with schema merging
//! - **Async I/O**: Fully async reading via `object_store`
//!
//! # Quick Start
//!
//! ## Using with DataFusion SessionContext
//!
//! ```rust,ignore
//! use datafusion::prelude::*;
//! use datafusion::datasource::listing::{
//!     ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
//! };
//! use datafusion_datasource_orc::OrcFormat;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> datafusion_common::Result<()> {
//!     // Create a SessionContext
//!     let ctx = SessionContext::new();
//!
//!     // Configure listing options with ORC format
//!     let listing_options = ListingOptions::new(Arc::new(OrcFormat::new()))
//!         .with_file_extension(".orc");
//!
//!     // Create a listing table URL
//!     let table_path = ListingTableUrl::parse("file:///path/to/orc/files/")?;
//!
//!     // Infer schema from the ORC files
//!     let schema = listing_options
//!         .infer_schema(&ctx.state(), &table_path)
//!         .await?;
//!
//!     // Create and register the table
//!     let config = ListingTableConfig::new(table_path)
//!         .with_listing_options(listing_options)
//!         .with_schema(schema);
//!     let table = ListingTable::try_new(config)?;
//!     ctx.register_table("my_orc_table", Arc::new(table))?;
//!
//!     // Query the table
//!     let df = ctx.sql("SELECT * FROM my_orc_table WHERE id > 100").await?;
//!     df.show().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Configuring Read Options
//!
//! ```rust
//! use datafusion_datasource_orc::{OrcFormat, OrcFormatOptions, OrcReadOptions};
//!
//! // Create read options
//! let read_options = OrcReadOptions::default()
//!     .with_batch_size(16384)           // Custom batch size
//!     .with_pushdown_predicate(true)    // Enable predicate pushdown
//!     .with_metadata_size_hint(1048576); // 1MB metadata hint
//!
//! // Create format with options
//! let format_options = OrcFormatOptions { read: read_options };
//! let format = OrcFormat::new().with_options(format_options);
//! ```
//!
//! # Architecture
//!
//! This crate follows DataFusion's file format abstraction:
//!
//! ```text
//! ┌─────────────────────┐
//! │   OrcFormatFactory  │  Creates OrcFormat instances
//! └──────────┬──────────┘
//!            │
//! ┌──────────▼──────────┐
//! │      OrcFormat      │  FileFormat trait implementation
//! └──────────┬──────────┘
//!            │ create_physical_plan()
//! ┌──────────▼──────────┐
//! │      OrcSource      │  FileSource trait implementation
//! └──────────┬──────────┘
//!            │ create_file_opener()
//! ┌──────────▼──────────┐
//! │      OrcOpener      │  Opens files and creates streams
//! └──────────┬──────────┘
//!            │
//! ┌──────────▼──────────┐
//! │ ObjectStoreChunkReader │  Adapts object_store to orc-rust
//! └──────────┬──────────┘
//!            │
//! ┌──────────▼──────────┐
//! │  orc-rust Reader    │  ORC file parsing
//! └─────────────────────┘
//! ```
//!
//! # Predicate Pushdown
//!
//! When a filter predicate is provided, this crate converts supported DataFusion
//! expressions to `orc-rust` predicates for stripe-level filtering:
//!
//! - Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
//! - Logical operators: `AND`, `OR`, `NOT`
//! - Null checks: `IS NULL`, `IS NOT NULL`
//!
//! Unsupported predicates are gracefully ignored (no error), and filtering
//! falls back to DataFusion's row-level evaluation.
//!
//! # Supported Data Types
//!
//! The following ORC types are supported via `orc-rust`:
//!
//! | ORC Type | Arrow Type |
//! |----------|------------|
//! | BOOLEAN | Boolean |
//! | BYTE | Int8 |
//! | SHORT | Int16 |
//! | INT | Int32 |
//! | LONG | Int64 |
//! | FLOAT | Float32 |
//! | DOUBLE | Float64 |
//! | STRING | Utf8 |
//! | BINARY | Binary |
//! | DECIMAL | Decimal128 |
//! | DATE | Date32 |
//! | TIMESTAMP | Timestamp |
//! | LIST | List |
//! | MAP | Map |
//! | STRUCT | Struct |
//!
//! [`FileFormat`]: datafusion_datasource::file_format::FileFormat
//! [`FileSource`]: datafusion_datasource::file::FileSource
//! [`orc-rust`]: https://github.com/datafusion-contrib/orc-rust
//! [`object_store`]: https://docs.rs/object_store

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod file_format;
pub mod metadata;
pub mod metrics;
pub mod options;
pub mod source;

mod opener;
mod predicate;
mod reader;
mod writer;

// Re-export main types
pub use file_format::{OrcFormat, OrcFormatFactory};
pub use metrics::OrcFileMetrics;
pub use options::{OrcFormatOptions, OrcReadOptions};
pub use reader::ObjectStoreChunkReader;
pub use source::OrcSource;
