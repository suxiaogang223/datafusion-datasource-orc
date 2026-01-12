# DataFusion ORC Datasource

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.73%2B-orange.svg)](https://www.rust-lang.org/)
[![crates.io](https://img.shields.io/crates/v/datafusion-datasource-orc)](https://crates.io/crates/datafusion-datasource-orc)

A DataFusion extension providing ORC (Optimized Row Columnar) file format support for Apache DataFusion.

## Overview

`datafusion-datasource-orc` adds comprehensive ORC file format support to Apache DataFusion, enabling efficient query execution on ORC data through predicate pushdown, column projection, and async I/O.

Built on top of [orc-rust](https://github.com/datafusion-contrib/orc-rust), it implements DataFusion's file format abstraction traits (`FileFormat`, `FileSource`, `FileOpener`) to provide a seamless experience similar to DataFusion's built-in Parquet support.

## Features

- **Schema Inference** - Automatically infer table schema from ORC files
- **Statistics Extraction** - Extract file-level statistics for query optimization
- **Predicate Pushdown** - Filter data at stripe level using ORC row indexes
- **Column Projection** - Push down column selection to read only needed data
- **Async I/O** - Non-blocking reads with support for S3, GCS, Azure, and local filesystems
- **Multi-file Schema Merging** - Seamlessly query across multiple ORC files

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
datafusion-datasource-orc = "0.0.1"
datafusion = "51"
```

## Quick Start

```rust
use datafusion::prelude::*;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion_datasource_orc::OrcFormat;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion_common::Result<()> {
    // Create a SessionContext
    let ctx = SessionContext::new();

    // Configure listing options with ORC format
    let listing_options = ListingOptions::new(Arc::new(OrcFormat::default()))
        .with_file_extension(".orc");

    // Create a listing table URL
    let table_path = ListingTableUrl::parse("file:///path/to/orc/files/")?;

    // Register the table
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options);
    let table = ListingTable::try_new(config)?;
    ctx.register_table("my_table", Arc::new(table))?;

    // Execute query with predicate pushdown
    let df = ctx.sql("SELECT * FROM my_table WHERE id > 100").await?;
    df.show().await?;

    Ok(())
}
```

## Configuration

Configure ORC reading behavior via format options:

```rust
use datafusion_datasource_orc::{OrcFormatFactory, OrcFormatOptions, OrcReadOptions};

let read_options = OrcReadOptions::default()
    .with_batch_size(16384)              // Rows per batch
    .with_pushdown_predicate(true)        // Enable predicate pushdown
    .with_metadata_size_hint(1_048_576);  // Metadata buffer hint

let format_options = OrcFormatOptions { read: read_options };
let orc_factory = OrcFormatFactory::new_with_options(format_options);

let ctx = SessionContext::new();
ctx.register_file_format("orc", Arc::new(orc_factory))?;
```

### Format Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `orc.batch_size` | `usize` | 1024 | Number of rows per RecordBatch |
| `orc.pushdown_predicate` | `bool` | true | Enable/disable predicate pushdown |
| `orc.metadata_size_hint` | `usize` | 32768 | Metadata allocation hint in bytes |

## Type Support

| ORC Type | Arrow Type | Status |
|----------|------------|--------|
| BOOLEAN | Boolean | ✅ |
| TINYINT | Int8 | ✅ |
| SMALLINT | Int16 | ✅ |
| INT | Int32 | ✅ |
| BIGINT | Int64 | ✅ |
| FLOAT | Float32 | ✅ |
| DOUBLE | Float64 | ✅ |
| STRING | String | ✅ |
| BINARY | Binary | ✅ |
| TIMESTAMP | Timestamp | ✅ |
| LIST | List | ✅ |
| MAP | Map | ✅ |
| STRUCT | Struct | ⏳ |
| DECIMAL | Decimal128 | ⏳ |
| DATE | Date32 | ⏳ |
| VARCHAR | String | ⏳ |
| CHAR | String | ⏳ |

## Architecture

```
SQL Query
    ↓
DataFusion Logical Plan
    ↓
DataFusion Physical Plan
    ↓
OrcFormat.create_physical_plan()
    ↓
DataSourceExec (using OrcSource)
    ↓
OrcOpener.open()
    ↓
orc-rust ArrowReader
    ↓
Arrow RecordBatch Stream
```

### Core Components

- **`OrcFormat`** - Implements `FileFormat` trait, provides schema inference and statistics
- **`OrcSource`** - Implements `FileSource` trait, handles predicate pushdown
- **`OrcOpener`** - Implements `FileOpener` trait, manages file opening and stream creation
- **`ObjectStoreChunkReader`** - Bridges DataFusion's `object_store` to `orc-rust`'s reader

## Development

### Building

```bash
cargo build
```

### Testing

```bash
# Run all tests
cargo test

# Run specific test module
cargo test --test basic_reading
cargo test --test predicate_pushdown
```

### Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench orc_query_sql -- full_table_scan
```

## TODO

- [ ] Additional ORC type support (STRUCT, DECIMAL, DATE, VARCHAR, CHAR)
- [ ] Enhanced error handling and edge case coverage
- [ ] Write support (query results to ORC format)
- [ ] Compression options configuration
- [ ] Performance optimizations
- [ ] Comprehensive integration tests

## Related Projects

- [Apache DataFusion](https://github.com/apache/datafusion) - Query engine core
- [orc-rust](https://github.com/datafusion-contrib/orc-rust) - ORC file format Rust implementation
- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory format

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

Built on top of the excellent [orc-rust](https://github.com/datafusion-contrib/orc-rust) library and inspired by DataFusion's Parquet implementation.
