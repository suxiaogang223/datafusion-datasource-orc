# DataFusion ORC Extension

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.73%2B-orange.svg)](https://www.rust-lang.org/)

A DataFusion extension providing ORC (Optimized Row Columnar) file format support for Apache DataFusion.

## Overview

`datafusion-orc-extension` is an independent extension project that adds comprehensive ORC file format support to the DataFusion query engine. Built on top of [orc-rust](https://github.com/datafusion-contrib/orc-rust), it provides functionality similar to DataFusion's Parquet support.

## Status

🚧 **Work in Progress** - This project is currently in early development. Core functionality is being implemented.

## Features

### Planned Features

- ✅ **Basic Reading**: Read ORC files from ObjectStore
- ✅ **Schema Inference**: Automatically infer table schema from ORC files
- ✅ **Statistics**: Extract file statistics for query optimization
- ✅ **Column Projection**: Read only required columns for better performance
- ⏳ **Predicate Pushdown**: Filter data at file level (planned)
- ⏳ **Write Support**: Write query results to ORC format (planned)

## Implementation Plan

### Phase 1: Project Setup ✅

- [x] Project structure initialization
- [x] Implementation plan documentation
- [ ] Configure Cargo.toml dependencies
- [ ] Create module structure

### Phase 2: Core FileFormat Implementation

- [ ] Implement `OrcFormatFactory`
  - Implement `FileFormatFactory` trait
  - Support configuration options
- [ ] Implement `OrcFormat`
  - Implement `FileFormat` trait
  - Schema inference (`infer_schema`)
  - Statistics extraction (`infer_stats`)
  - Physical plan creation (`create_physical_plan`)

### Phase 3: FileSource Implementation

- [ ] Implement `OrcSource`
  - Implement `FileSource` trait
  - Statistics support
- [ ] Implement `OrcOpener`
  - Implement `FileOpener` trait
  - Async file reading
  - RecordBatch stream generation

### Phase 4: Reading Functionality

- [ ] Basic reading
  - ObjectStore integration
  - ORC file parsing
- [ ] Schema inference
  - ORC schema → Arrow schema conversion
  - Multi-file schema merging
- [ ] Statistics extraction
  - Stripe-level statistics
  - Column-level statistics (min/max/null count)
- [ ] Column projection support
  - Use orc-rust's `ProjectionMask`
- [ ] Predicate pushdown (advanced feature)
  - Stripe-level filtering
  - Row-level filtering

### Phase 5: Writing Functionality (Optional)

- [ ] Basic writing
  - Arrow RecordBatch → ORC file
  - ObjectStore integration
- [ ] Write configuration
  - Compression options (snappy, zlib, lz4, zstd, etc.)
  - Stripe size configuration
  - Row index stride configuration

### Phase 6: Testing and Documentation

- [ ] Unit tests
  - Schema inference tests
  - Reading functionality tests
  - Error handling tests
- [ ] Integration tests
  - End-to-end reading tests
  - SQL query tests
- [ ] Example code
  - Basic usage examples
  - SQL query examples
- [ ] API documentation

## Project Structure

```
datafusion-orc-extension/
├── Cargo.toml              # Project configuration and dependencies
├── README.md               # Project documentation
├── IMPLEMENTATION_PLAN.md  # Detailed implementation plan
├── src/
│   ├── lib.rs              # Library entry point
│   ├── file_format.rs      # FileFormat and FileFormatFactory implementation
│   ├── source.rs           # FileSource implementation
│   ├── reader.rs           # ORC file reading logic
│   ├── writer.rs           # ORC file writing logic (optional)
│   ├── metadata.rs         # ORC metadata processing
│   └── opener.rs           # File opening and configuration logic
└── tests/                  # Test files
    ├── integration/        # Integration tests
    └── unit/               # Unit tests
```

## Dependencies

### Core Dependencies

- **datafusion-common**: DataFusion common functionality
- **datafusion-datasource**: DataFusion datasource abstractions
- **datafusion-execution**: DataFusion execution engine
- **datafusion-physical-plan**: DataFusion physical plan
- **datafusion-session**: DataFusion session management
- **orc-rust**: Rust implementation of ORC file format
- **arrow**: Apache Arrow in-memory format
- **object_store**: Object storage abstraction layer

### Development Dependencies

- **tokio**: Async runtime
- **futures**: Async programming utilities
- **async-trait**: Async trait support

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datafusion-orc-extension = { path = "../datafusion-orc-extension" }
```

Or from git (when available):

```toml
[dependencies]
datafusion-orc-extension = { git = "https://github.com/your-org/datafusion-orc-extension" }
```

## Usage

### Basic Example

```rust
use datafusion_orc_extension::OrcFormatFactory;
use datafusion::prelude::*;

// Create SessionContext
let ctx = SessionContext::new();

// Register ORC format
let orc_factory = OrcFormatFactory::new();
ctx.register_file_format("orc", orc_factory);

// Read ORC file
ctx.register_table(
    "my_table",
    ctx.read_table("file:///path/to/file.orc")?
)?;

// Execute query
let df = ctx.sql("SELECT * FROM my_table WHERE column > 100").await?;
df.show().await?;
```

> **Note**: This is example code for future implementation. The API may change during development.

## Architecture

### Core Components

1. **OrcFormat**: Implements `FileFormat` trait, provides file format abstraction
2. **OrcFormatFactory**: Implements `FileFormatFactory` trait, creates format instances
3. **OrcSource**: Implements `FileSource` trait, provides datasource functionality
4. **OrcOpener**: Implements `FileOpener` trait, handles file opening and data streams

### Data Flow

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

## Development

### Requirements

- Rust 1.73+ (matching orc-rust requirements)
- Latest DataFusion version
- orc-rust (located at `../orc-rust`)

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

### Code Style

- Follow Rust official code style
- Format code with `cargo fmt`
- Check code quality with `cargo clippy`

## Contributing

Contributions are welcome! Please see [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for detailed implementation plans.

### How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Roadmap

- [ ] **v0.1.0**: Basic reading functionality
- [ ] **v0.2.0**: Schema inference and statistics
- [ ] **v0.3.0**: Column projection and basic optimizations
- [ ] **v0.4.0**: Predicate pushdown support
- [ ] **v0.5.0**: Writing functionality
- [ ] **v1.0.0**: Production-ready version

## Related Projects

- [Apache DataFusion](https://github.com/apache/datafusion) - Query engine core
- [orc-rust](https://github.com/datafusion-contrib/orc-rust) - ORC file format Rust implementation
- [Apache Arrow](https://arrow.apache.org/) - Columnar in-memory format

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built on top of the excellent [orc-rust](https://github.com/datafusion-contrib/orc-rust) library
- Inspired by DataFusion's Parquet implementation
- Part of the Apache DataFusion ecosystem

---

**Note**: This project is currently in early development. APIs may change.
