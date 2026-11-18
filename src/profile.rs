// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT and LICENSE-APACHE in the project root.

//! Opt-in performance counters, enabled by setting the `CODCEL_PROFILE` environment variable.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("CODCEL_PROFILE").is_some())
}

/// SQL queries requested.
pub static SQL_QUERIES: AtomicU64 = AtomicU64::new(0);
/// SQL queries answered from the result cache.
pub static SQL_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
/// Tables read into memory.
pub static MEMORY_TABLES: AtomicU64 = AtomicU64::new(0);
/// Nanoseconds spent planning and executing SQL queries in DataFusion.
pub static SQL_EXEC_NANOS: AtomicU64 = AtomicU64::new(0);

#[inline]
pub(crate) fn add(counter: &AtomicU64, amount: u64) {
    if enabled() {
        counter.fetch_add(amount, Ordering::Relaxed);
    }
}

/// Returns the counters as (name, value) pairs and resets them.
pub fn take() -> Vec<(&'static str, u64)> {
    vec![
        ("sql_queries", SQL_QUERIES.swap(0, Ordering::Relaxed)),
        ("sql_cache_hits", SQL_CACHE_HITS.swap(0, Ordering::Relaxed)),
        ("memory_tables", MEMORY_TABLES.swap(0, Ordering::Relaxed)),
        (
            "sql_exec_ms",
            SQL_EXEC_NANOS.swap(0, Ordering::Relaxed) / 1_000_000,
        ),
    ]
}
