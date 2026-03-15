<p align="center">
  <a href="https://codcel.io">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="assets/codcel-logo-lockup-dark.svg">
      <img src="assets/codcel-logo-lockup.svg" alt="Codcel" width="320">
    </picture>
  </a>
</p>

# Codcel Parquet Engine

Read-only Parquet table engine for Codcel — Excel-like lookups and filtering on columnar data files, with query caching and request coalescing.

## Overview

Codcel Parquet Engine implements the [`CodcelTable`](https://github.com/codcel-io/codcel-table-engine) trait for Apache Parquet files. It provides Excel-compatible lookup and filtering operations on columnar data, powered by [DataFusion](https://datafusion.apache.org/) for SQL execution. Designed for analytics, data lakes, and high-volume read-only workloads.

This is one of the open-source components of [Codcel](https://codcel.io). Codcel converts your Excel spreadsheets into clean, human-readable source code — in Rust, Python, Java, C#, TypeScript, Go, Swift, and more. You get the full source code, and this engine is part of what you get: your generated projects use it directly for lightning-fast reads on columnar data — no proprietary lock-in.

## Features

- **Excel-compatible lookups** — VLOOKUP, HLOOKUP, XLOOKUP, LOOKUP, MATCH, XMATCH, INDEX, FILTER
- **SQL query caching** — configurable TTL (default 300s) with automatic cleanup
- **Request coalescing** — concurrent identical queries share a single execution via a leader/follower broadcast pattern
- **Sharded Parquet support** — glob patterns for partitioned datasets (e.g. `table_*.parquet`)
- **SQL injection protection** — identifier validation and string escaping at the query layer
- **Read-only by design** — optimized for query performance; CRUD operations are not supported

## Quick Start

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
codcel-parquet-engine = { git = "https://github.com/codcel-io/codcel-parquet-engine.git", branch = "main" }
```

Initialize a table from a Parquet file:

```rust
use codcel_parquet_engine::parquet_table::ParquetTable;

let table = ParquetTable::init("data/sales.parquet".to_string(), "sales").await?;
```

Then use any `CodcelTable` operation — `v_lookup`, `x_lookup`, `filter`, `index`, and more.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

## About Codcel

[Codcel](https://codcel.io) turns Excel spreadsheets into production-ready software — real source code in Rust, Python, Java, C#, TypeScript, Go, Swift, and more, with zero platform lock-in.

This Parquet engine is one of several open-source components that power Codcel. Learn more at [codcel.io](https://codcel.io).

## Licensing

This project is available under **multiple licenses**:

- **MIT License** (LICENSE-MIT)
- **Apache License 2.0** (LICENSE-APACHE)
- **Codcel Commercial License** (LICENSE-CODCEL-COMMERCIAL)

This licensing model is intended to keep the Excel calculation logic
open and auditable, while protecting Codcel from direct competition
in automated spreadsheet-to-code platforms.

### Which license should I use?

- If you are an individual developer, company, or library author using
  these Excel functions directly in your application, you may choose
  **MIT or Apache-2.0**.

- If you are using this software as part of an **Automated Code Generation
  System (excluding Codcel itself)**, you must use the
  **Codcel Commercial License**.

See LICENSE-CODCEL-COMMERCIAL for details.
