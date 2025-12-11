# AGENTS.md

This file provides guidance to coding agents collaborating on this repository.

## Project Overview

`datafusion-orc-extension` adds Apache ORC file-format support to Apache DataFusion. The extension includes:

- DataFusion `FileFormat`, `FileSource`, and `FileOpener` implementations.
- Async ORC readers built on top of [`orc-rust`](https://github.com/datafusion-contrib/orc-rust).
- Integration tests and sample ORC fixtures (`tests/integration/data/`) verifying schema inference, RecordBatch streaming, and projection behaviors.

## Project Vision

Deliver a first-class ORC experience inside DataFusion: portable, performant, async-ready, and aligned with DataFusion’s APIs.

## Project Requirements

- Keep code concise, maintainable, and idiomatic Rust 2021.
- Use English for code, comments, docs, and examples.
- Only add meaningful comments/tests; avoid noise.
- Favor DataFusion abstractions and async patterns to ensure portability.

## Repository Structure Highlights

- `src/file_format.rs` – DataFusion `FileFormat`/factory wiring.
- `src/source.rs` – `FileSource` implementation integrating with `FileScanConfig`.
- `src/opener.rs` – `FileOpener` that builds async ORC readers and emits `RecordBatch` streams.
- `src/metadata.rs`, `src/reader.rs`, etc. – Helpers for metadata, async chunk readers, statistics.
- `tests/basic_reading.rs` – Integration tests covering schema inference, projection, and aggregations.
- `tests/integration/data/` – Sample ORC files used by the tests (keep additions minimal).

## Common Development Commands

### Rust

- Format: `cargo fmt`
- Lint: `cargo clippy --all-features`
- Test entire workspace: `cargo test`
- Run a specific test: `cargo test <test_name>`
- Run integration test verbosely: `cargo test <test_name> -- --nocapture`

### General Tips

- Use `ObjectStorePath::from_filesystem_path` when converting local paths to avoid platform issues.
- When projections or statistics are missing, fall back to safe defaults (e.g., `Statistics::new_unknown`) rather than panicking.
- Keep binary fixtures small; document datasets in test comments if they represent specific scenarios.

## Architecture Notes

1. **Async-first**: All file reading is async via tokio/object_store—do not introduce blocking I/O.
2. **Arrow-native**: ORC data is converted to Arrow arrays/batches; ensure schemas align throughout.
3. **Projection Handling**: `FileScanConfig` supplies projected columns; respect them when building readers, and re-project batches if ORC-level pushdown is unavailable.
4. **Statistics**: Provide at least “unknown” statistics so DataFusion can operate without panics.
5. **Object Stores**: Rely on DataFusion’s `object_store` abstraction to remain compatible with local and remote storage backends.

## Development Notes

- Every feature or bug fix must include tests (unit and/or integration).
- Update `README.md`/docs whenever capabilities or roadmap items change.
- Prefer builder patterns or options structs over long parameter lists for public APIs.
- Document public APIs with Rustdoc examples where practical.
- Avoid collecting entire streams into memory; keep pipelines streaming whenever possible.

## Review Checklist for Agents

- Does the change compile (`cargo check`) and pass fmt/clippy/test?
- Are projections/statistics/path conversions handled safely?
- Are new tests added for new functionality/regressions?
- Are docs/README updated if behavior changed?
- Is code idiomatic, async-safe, and portable (no OS-specific hacks)?
