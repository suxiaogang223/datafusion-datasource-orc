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
//! [`FileFormat`]: datafusion_datasource::file_format::FileFormat
//! [`FileSource`]: datafusion_datasource::file::FileSource
//! [`orc-rust`]: https://github.com/datafusion-contrib/orc-rust
//! [`object_store`]: https://docs.rs/object_store

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod file_format;
pub mod metadata;
pub mod options;
pub mod source;

mod opener;
mod predicate;
mod reader;
mod writer;

// Re-export main types
pub use file_format::{OrcFormat, OrcFormatFactory};
pub use options::{OrcFormatOptions, OrcReadOptions};
pub use reader::ObjectStoreChunkReader;
pub use source::OrcSource;
