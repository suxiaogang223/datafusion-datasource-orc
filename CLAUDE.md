# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`datafusion-datasource-orc` is a DataFusion extension providing ORC file format support. Built on `orc-rust`, it implements DataFusion's file format abstraction traits (`FileFormat`, `FileSource`, `FileOpener`) to enable ORC reading with predicate pushdown, projection pushdown, and async I/O.

**Current Status**: Phase 4c (Predicate Pushdown) completed. Phase 4a (Basic Reading) in progress - unit tests and error handling pending.

## Common Commands

```bash
# Build
cargo build

# Run all tests
cargo test

# Run specific test module
cargo test --test basic_reading
cargo test --test predicate_pushdown

# Run specific test function
cargo test test_schema_inference
cargo test test_predicate_pushdown_simple

# Format code
cargo fmt

# Lint
cargo clippy

# Run benchmarks (Criterion)
cargo bench
cargo bench --bench orc_query_sql -- full_table_scan

# Run TPC-DS benchmark
cargo run --release --bin tpcds_bench -- --path /path/to/orc/files --query 1 --iterations 3
```

## Architecture

The codebase follows DataFusion's trait-based architecture for file format plugins:

```
OrcFormatFactory (FileFormatFactory)
    │
    ▼
OrcFormat (FileFormat)
    │ create_physical_plan()
    ▼
OrcSource (FileSource)
    │ create_file_opener()
    ▼
OrcOpener (FileOpener)
    │
    ▼
ObjectStoreChunkReader (bridges object_store to orc-rust)
    │
    ▼
orc-rust ArrowReader
```

### Key Components

**`src/file_format.rs`**: `OrcFormat` and `OrcFormatFactory`
- `OrcFormatFactory`: Creates `OrcFormat` instances, parses DataFusion format options
- `OrcFormat`: Implements `FileFormat` trait - schema inference, stats extraction, physical plan creation
- Format options: `orc.batch_size`, `orc.pushdown_predicate`, `orc.metadata_size_hint`

**`src/source.rs`**: `OrcSource`
- Implements `FileSource` trait
- Manages scan-level configuration (projections, limits, predicates)
- Converts DataFusion expressions to ORC predicates via `OrcSource::filter()`
- Tracks metrics via `ExecutionPlanMetricsSet`

**`src/opener.rs`**: `OrcOpener`
- Implements `FileOpener` trait
- Opens ORC files asynchronously, creates `SendableRecordBatchStream`
- Applies projection mask and limit pushdown
- Configures batch size and predicates on ArrowReaderBuilder

**`src/reader.rs`**: `ObjectStoreChunkReader`
- Adapts DataFusion's `object_store` to `orc-rust`'s `AsyncChunkReader` trait
- Enables async I/O for any object_store backend (local, S3, GCS, Azure)
- Tracks I/O metrics (bytes scanned, I/O requests)

**`src/predicate.rs`**: Predicate conversion
- Converts DataFusion `PhysicalExpr` to `orc-rust` `Predicate`
- Supported: `=`, `!=`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`, `NOT`
- Unsupported predicates return `None` (graceful fallback to DataFusion row-level filtering)

**`src/metadata.rs`**: Schema and metadata processing
- Reads ORC file footer and converts ORC schema to Arrow schema
- Extracts file-level statistics (row count, file size)
- Handles multi-file schema merging

**`src/options.rs`**: Configuration
- `OrcReadOptions`: Batch size, predicate pushdown enable/disable, metadata size hint
- `OrcFormatOptions`: Top-level options container

**`src/metrics.rs`**: Performance monitoring
- Tracks: I/O bytes/requests, metadata time, stripe pruning, predicate evaluation, decode time
- All metrics reported via DataFusion's `ExecutionPlanMetricsSet`

## Trait Implementation Pattern

When implementing DataFusion traits, follow these patterns:

1. **FileFormatFactory**: Parse format options, create configured `FileFormat` instances
2. **FileFormat**: Return `OrcSource` from `create_physical_plan()`, handle schema/stats inference
3. **FileSource**: Return `OrcOpener` from `create_file_opener()`, implement `filter()` for predicate pushdown
4. **FileOpener**: Return `SendableRecordBatchStream` from `open()`, apply projection/limit/predicate

## Testing

Integration tests use sample ORC files in `tests/`:
- `tests/basic_reading.rs`: Schema inference, streaming, projection+LIMIT, complex types (map/list)
- `tests/predicate_pushdown.rs`: Predicate conversion and filtering effectiveness

Test files are loaded relative to `tests/` directory using `std::path::Path::new("alltypes.snappy.orc")`.

## Predicate Pushdown

Predicate pushdown works in two stages:

1. **OrcSource::filter()**: Called by DataFusion to check if predicates can be pushed down
   - Converts `PhysicalExpr` to `orc-rust::Predicate`
   - Returns `Some(Predicate)` if supported, `None` if unsupported

2. **OrcOpener**: Applies the stored predicate to ArrowReaderBuilder
   - Uses `builder.with_predicate(predicate)` to enable stripe-level filtering

Unsupported predicates are safely ignored - they fall back to DataFusion's row-level evaluation.

## Configuration Options

Format options are parsed in `OrcFormatFactory::try_new()`:
- `orc.batch_size` (usize): RecordBatch row count (default: 1024)
- `orc.pushdown_predicate` (bool): Enable/disable predicate pushdown (default: true)
- `orc.metadata_size_hint` (usize): Metadata allocation hint (default: 32KB)

## Dependencies

- DataFusion 51.0.0+ (must match across all datafusion-* crates)
- orc-rust 0.7.1+ (with `async` feature)
- Arrow 57.1.0+
- object_store 0.12.4+

## Code Conventions

- All source files include Apache 2.0 license headers
- `#![deny(missing_docs)]` is enabled - document all public APIs
- Use `?` for error propagation, prefer `Result<T>` types
- All I/O operations are async (`async fn`, `.await`)
- Public types are re-exported from `lib.rs` for clean external API
