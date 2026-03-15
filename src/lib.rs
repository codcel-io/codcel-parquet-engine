// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0 OR Codcel-Commercial
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT, LICENSE-APACHE, and LICENSE-CODCEL-COMMERCIAL in the project root.

//! Parquet table implementation for Codcel.
//!
//! This crate provides a read-only table implementation backed by Parquet files,
//! offering Excel-like lookup, filter, and search operations. It implements the
//! `CodcelTable` trait from `codcel-table-engine`, enabling seamless integration
//! with the Codcel spreadsheet computation engine.
//!
//! # Features
//!
//! - **Excel-compatible operations**: VLOOKUP, HLOOKUP, XLOOKUP, INDEX, MATCH, XMATCH, FILTER
//! - **SQL query caching**: Automatic caching with configurable TTL for improved performance
//! - **Request coalescing**: Concurrent identical queries share execution to reduce load
//! - **Sharded file support**: Transparently handles tables split across multiple Parquet files
//! - **SQL injection protection**: Validates all identifiers and escapes string literals
//!
//! # Main Types
//!
//! - [`ParquetTable`]: The main entry point for working with Parquet-backed tables
//! - [`parquet_table::RowColumnValues`]: Type alias for 2D result sets (rows of column values)

pub mod parquet_table;
mod sql_cache;

pub use parquet_table::ParquetTable;
