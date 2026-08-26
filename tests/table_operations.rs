// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT and LICENSE-APACHE in the project root.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test scaffolding: clippy's allow-*-in-tests keys do not reach \
              the plain helper functions an integration test target is built from"
)]

//! Behavioural snapshot suite for `ParquetTable`.
//!
//! Generates deterministic Parquet fixtures into `CARGO_TARGET_TMPDIR`, exercises the
//! whole `CodcelTable` surface against them, and compares the resulting transcript to
//! an expected snapshot. Any change in query construction, type mapping, result
//! extraction, or the behaviour of the underlying DataFusion / Arrow stack shows up as
//! a transcript diff.
//!
//! The suite is hermetic: no network, no external fixture files, no environment
//! variables. Run it with `cargo test`.
//!
//! The sharded (`shard`) and `DISTINCT` cases specifically cover results that arrive as
//! more than one `RecordBatch`. That path is the most sensitive to the query engine
//! changing how it splits batches across files and partitions, and it previously
//! mis-assembled such results by appending every batch onto the first batch's rows.
//!
//! Rows within a multi-batch result have no guaranteed order — batches are produced by a
//! parallel scan — so those cases are compared order-insensitively via `case_unordered`.
//!
//! To review an intentional behaviour change, run with `SNAPSHOT=print`, inspect the
//! emitted transcript, and paste it into `EXPECTED`.

use codcel_calculation_engine::input::Input;
use codcel_calculation_engine::value::Value;
use codcel_calculation_engine::value_format::ValueFormat;
use codcel_parquet_engine::ParquetTable;
use codcel_table_engine::codcel_table::CodcelTable;
use codcel_table_engine::condition::{Condition, ConditionValue, WildcardPosition};
use codcel_table_engine::sql_modifiers::{SqlAggregate, SqlModifiers};
use codcel_table_engine::table_functions::TableFunctions;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use std::error::Error;
use std::fmt::Write as _;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Fixture generation
// ---------------------------------------------------------------------------

const CITIES: [&str; 4] = ["Lisboa", "Porto", "Faro", "Braga"];
const REGIONS: [&str; 3] = ["Norte", "Centro", "Sul"];

/// Writes a single-shard Parquet file at `<dir>/<base>_xyz<shard>.parquet`.
fn write_parquet(
    dir: &Path,
    base: &str,
    shard: usize,
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
) {
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("build record batch");
    let path = dir.join(format!("{base}_xyz{shard}.parquet"));
    let file = File::create(&path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("open parquet writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close parquet writer");
}

/// Postcode-shaped table: int key, five string columns, one float column.
/// Rows are generated from `range`, so the same helper builds both the single-file
/// `post` table and the two shards of `shard`.
fn postcode_columns(range: std::ops::Range<usize>) -> (Arc<Schema>, Vec<ArrayRef>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, false),
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Utf8, false),
        Field::new("c3", DataType::Utf8, false),
        Field::new("c4", DataType::Utf8, false),
        Field::new("c5", DataType::Utf8, false),
        Field::new("c6", DataType::Float64, false),
    ]));

    let c0: Vec<i32> = range.clone().map(|i| (i + 1) as i32).collect();
    let c1: Vec<String> = range.clone().map(|i| format!("P{:04}", i + 1)).collect();
    let c2: Vec<String> = range
        .clone()
        .map(|i| CITIES[i % CITIES.len()].to_string())
        .collect();
    let c3: Vec<String> = c2.iter().map(|s| s.to_uppercase()).collect();
    let c4: Vec<String> = range
        .clone()
        .map(|i| REGIONS[i % REGIONS.len()].to_string())
        .collect();
    let c5: Vec<String> = range.clone().map(|i| format!("D{:02}", i % 20)).collect();
    // Exactly-reproducible decimals in [0.00, 2.99] with no accumulated float drift.
    let c6: Vec<f64> = range.map(|i| ((i * 37) % 300) as f64 / 100.0).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(c0)),
        Arc::new(StringArray::from(c1)),
        Arc::new(StringArray::from(c2)),
        Arc::new(StringArray::from(c3)),
        Arc::new(StringArray::from(c4)),
        Arc::new(StringArray::from(c5)),
        Arc::new(Float64Array::from(c6)),
    ];
    (schema, columns)
}

/// Creates every fixture used by the suite and returns the directory holding them.
fn build_fixtures() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("parquet-fixtures");
    std::fs::create_dir_all(&dir).expect("create fixture dir");

    // `accel`: two numeric columns. Values are quarter-steps so every value is exactly
    // representable as f64 and compares cleanly against SQL literals.
    let accel_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Float64, false),
        Field::new("c2", DataType::Float64, false),
    ]));
    let c1: Vec<f64> = (0..81).map(|i| 2.0 + i as f64 * 0.25).collect();
    let c2: Vec<f64> = (0..81).map(|i| i as f64 * 0.5).collect();
    write_parquet(
        &dir,
        "accel",
        0,
        accel_schema,
        vec![
            Arc::new(Float64Array::from(c1)),
            Arc::new(Float64Array::from(c2)),
        ],
    );

    // `cars`: mixed string / int / float columns for type-mapping coverage.
    let cars_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Utf8, false),
        Field::new("c3", DataType::Utf8, false),
        Field::new("c4", DataType::Int32, false),
        Field::new("c5", DataType::Float64, false),
    ]));
    const MAKES: [&str; 4] = ["Abarth", "BMW", "Tesla", "Volvo"];
    const BODIES: [&str; 3] = ["Convertible", "Hatchback", "Saloon"];
    let n = 100usize;
    let cars: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            (0..n)
                .map(|i| MAKES[i % MAKES.len()].to_string())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            (0..n).map(|i| format!("M{:03}", i)).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            (0..n)
                .map(|i| BODIES[i % BODIES.len()].to_string())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            (0..n).map(|i| (100 + i) as i32).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            (0..n).map(|i| (i % 40) as f64 * 0.25).collect::<Vec<_>>(),
        )),
    ];
    write_parquet(&dir, "cars", 0, cars_schema, cars);

    // `post`: single-file postcode table.
    let (schema, columns) = postcode_columns(0..500);
    write_parquet(&dir, "post", 0, schema, columns);

    // `shard`: the same shape split across two shards, exercising the `_xyz*` glob.
    let (schema, columns) = postcode_columns(0..200);
    write_parquet(&dir, "shard", 0, schema, columns);
    let (schema, columns) = postcode_columns(200..300);
    write_parquet(&dir, "shard", 1, schema, columns);

    dir
}

// ---------------------------------------------------------------------------
// Deterministic rendering
// ---------------------------------------------------------------------------

/// Renders a scalar value in full; collapses collections to size plus their first and
/// last elements so the snapshot stays readable while remaining sensitive to changes in
/// ordering, row count, or element type.
fn show(v: &Value) -> String {
    match v {
        Value::VecValue(items) => match (items.first(), items.last()) {
            (Some(f), Some(l)) => format!(
                "Vec[n={}; first={}; last={}]",
                items.len(),
                show(f),
                show(l)
            ),
            _ => "Vec[n=0]".to_string(),
        },
        Value::AreaValue(rows) => match (rows.first(), rows.last()) {
            (Some(f), Some(l)) => format!(
                "Area[rows={}, cols={}; first={}; last={}]",
                rows.len(),
                f.len(),
                f.iter().map(show).collect::<Vec<_>>().join(","),
                l.iter().map(show).collect::<Vec<_>>().join(","),
            ),
            _ => "Area[rows=0]".to_string(),
        },
        other => format!("{other:?}"),
    }
}

fn render(r: Result<Value, Box<dyn Error + Send + Sync>>) -> String {
    match r {
        Ok(v) => format!("OK   {}", show(&v)),
        Err(e) => format!("ERR  {e}"),
    }
}

/// Order-insensitive rendering for results assembled from more than one `RecordBatch`.
///
/// A multi-shard table is scanned in parallel, so the order in which batches arrive —
/// and therefore the order of rows and of values within a row — varies between runs.
/// Sorting both makes the snapshot stable while still pinning row count, column count,
/// and the exact multiset of values.
fn show_unordered(v: &Value) -> String {
    match v {
        Value::AreaValue(rows) => {
            let cols = rows.first().map(|r| r.len()).unwrap_or(0);
            let mut rendered: Vec<String> = rows
                .iter()
                .map(|row| {
                    let mut vals: Vec<String> = row.iter().map(show).collect();
                    vals.sort();
                    vals.join(",")
                })
                .collect();
            rendered.sort();
            format!(
                "Area[rows={}, cols={}; min={}; max={}]",
                rows.len(),
                cols,
                rendered.first().cloned().unwrap_or_default(),
                rendered.last().cloned().unwrap_or_default(),
            )
        }
        other => show(other),
    }
}

fn render_unordered(r: Result<Value, Box<dyn Error + Send + Sync>>) -> String {
    match r {
        Ok(v) => format!("OK   {}", show_unordered(&v)),
        Err(e) => format!("ERR  {e}"),
    }
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

struct Transcript(String);

impl Transcript {
    fn section(&mut self, name: &str) {
        let _ = writeln!(self.0, "\n=== {name} ===");
    }
    fn case(&mut self, name: &str, result: Result<Value, Box<dyn Error + Send + Sync>>) {
        let _ = writeln!(self.0, "{name:<44} {}", render(result));
    }
    /// For results spanning several `RecordBatch`es, whose arrival order is not stable.
    fn case_unordered(&mut self, name: &str, result: Result<Value, Box<dyn Error + Send + Sync>>) {
        let _ = writeln!(self.0, "{name:<44} {}", render_unordered(result));
    }
    fn note(&mut self, name: &str, detail: impl std::fmt::Display) {
        let _ = writeln!(self.0, "{name:<44} {detail}");
    }
}

async fn open(dir: &Path, base: &str) -> ParquetTable {
    ParquetTable::init(
        dir.join(format!("{base}.parquet"))
            .to_string_lossy()
            .into_owned(),
        &format!("{base}.parquet"),
    )
    .await
    .unwrap_or_else(|e| panic!("init {base}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_operations_snapshot() {
    let dir = build_fixtures();

    // from_language is deterministic, so the snapshot does not depend on the host
    // locale or on CODCEL_* environment variables.
    let vf = ValueFormat::from_language("en-US");
    let input = Input::new("test", vf.clone());
    let f = TableFunctions::none();
    let vfr = &vf;

    let accel = open(&dir, "accel").await;
    let cars = open(&dir, "cars").await;
    let post = open(&dir, "post").await;
    let shard = open(&dir, "shard").await;

    let mut t = Transcript(String::new());

    t.section("VLOOKUP");
    t.case(
        "numeric exact hit",
        accel
            .v_lookup("2.25", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.case(
        "numeric exact miss",
        accel
            .v_lookup("2.30", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.case(
        "numeric range (<=)",
        accel
            .v_lookup("2.30", "c2", "c1", Some(true), &f, &input, vfr)
            .await,
    );
    t.case(
        "numeric range default arg",
        accel
            .v_lookup("3.0", "c2", "c1", None, &f, &input, vfr)
            .await,
    );
    t.case(
        "numeric below minimum",
        accel
            .v_lookup("0.5", "c2", "c1", Some(true), &f, &input, vfr)
            .await,
    );
    t.case(
        "multi-column result",
        accel
            .v_lookup("2.25", "c1,c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.case(
        "string exact",
        cars.v_lookup("Abarth", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.case(
        "string case-insensitive",
        cars.v_lookup("aBaRtH", "c3", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.case(
        "string miss",
        cars.v_lookup("Nonexistent", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.case(
        "sharded table",
        shard
            .v_lookup("P0250", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.case(
        "rejects sql injection in column",
        accel
            .v_lookup(
                "2.25",
                "c2; DROP TABLE x",
                "c1",
                Some(false),
                &f,
                &input,
                vfr,
            )
            .await,
    );
    t.case(
        "rejects non-numeric on numeric col",
        accel
            .v_lookup("not-a-number", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.case(
        "escapes quote in string literal",
        cars.v_lookup("O'Brien", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );

    t.section("MATCH");
    t.case(
        "exact (0)",
        post.match_table("P0010", Some(0), "c1", 0, vfr).await,
    );
    t.case(
        "exact miss",
        post.match_table("zzzz", Some(0), "c1", 0, vfr).await,
    );
    t.case(
        "largest <= (1)",
        post.match_table("P0010", Some(1), "c1", 0, vfr).await,
    );
    t.case(
        "default match_type",
        post.match_table("P0010", None, "c1", 0, vfr).await,
    );
    t.case(
        "smallest >= (-1)",
        post.match_table("P0010", Some(-1), "c1", 0, vfr).await,
    );
    t.case(
        "numeric column",
        post.match_table("2.78", Some(0), "c6", 0, vfr).await,
    );
    t.case(
        "invalid match_type",
        post.match_table("P0010", Some(7), "c1", 0, vfr).await,
    );
    t.case(
        "sharded table",
        shard.match_table("P0250", Some(0), "c1", 0, vfr).await,
    );
    t.case(
        "horizontal row search",
        post.match_table("Lisboa", Some(0), "c1,c2,c3,c4,c5", 1, vfr)
            .await,
    );

    t.section("INDEX");
    t.case(
        "row 1 column 2",
        post.index(1, Some(2), &f, &input, vfr).await,
    );
    t.case(
        "row 5 last column",
        post.index(5, Some(6), &f, &input, vfr).await,
    );
    t.case(
        "column beyond end",
        post.index(5, Some(7), &f, &input, vfr).await,
    );
    t.case("whole row", post.index(1, None, &f, &input, vfr).await);
    t.case(
        "row beyond end",
        post.index(99999, Some(2), &f, &input, vfr).await,
    );
    t.case(
        "row 0 (whole column)",
        post.index(0, Some(2), &f, &input, vfr).await,
    );
    t.case(
        "negative row",
        post.index(-3, Some(2), &f, &input, vfr).await,
    );
    t.case(
        "row spanning shards",
        shard.index(250, Some(2), &f, &input, vfr).await,
    );

    t.section("HLOOKUP");
    t.case(
        "exact",
        post.h_lookup("P0001", 2, Some(false), &f, &input, "c1", vfr)
            .await,
    );
    t.case(
        "range",
        post.h_lookup("P0001", 3, Some(true), &f, &input, "c1", vfr)
            .await,
    );
    t.case(
        "miss",
        post.h_lookup("nope", 2, Some(false), &f, &input, "c1", vfr)
            .await,
    );

    t.section("XLOOKUP");
    for (label, mm) in [
        ("exact (0)", 0),
        ("next smallest (-1)", -1),
        ("next largest (1)", 1),
    ] {
        t.case(
            &format!("match mode {label}"),
            post.x_lookup(
                "P0010",
                "c1",
                "c2",
                0,
                None,
                Some(mm),
                Some(1),
                &f,
                &input,
                vfr,
            )
            .await,
        );
    }
    t.case(
        "wildcard mode (2)",
        post.x_lookup(
            "P001*",
            "c1",
            "c2",
            0,
            None,
            Some(2),
            Some(1),
            &f,
            &input,
            vfr,
        )
        .await,
    );
    t.case(
        "search first",
        post.x_lookup(
            "Lisboa",
            "c2",
            "c1",
            0,
            None,
            Some(0),
            Some(1),
            &f,
            &input,
            vfr,
        )
        .await,
    );
    t.case(
        "search reverse",
        post.x_lookup(
            "Lisboa",
            "c2",
            "c1",
            0,
            None,
            Some(0),
            Some(-1),
            &f,
            &input,
            vfr,
        )
        .await,
    );
    t.case(
        "binary first",
        post.x_lookup(
            "P0010",
            "c1",
            "c2",
            0,
            None,
            Some(0),
            Some(2),
            &f,
            &input,
            vfr,
        )
        .await,
    );
    t.case(
        "binary last",
        post.x_lookup(
            "P0010",
            "c1",
            "c2",
            0,
            None,
            Some(0),
            Some(-2),
            &f,
            &input,
            vfr,
        )
        .await,
    );
    t.case(
        "if_not_found supplied",
        post.x_lookup(
            "zzz",
            "c1",
            "c2",
            0,
            Some("MISSING".into()),
            Some(0),
            Some(1),
            &f,
            &input,
            vfr,
        )
        .await,
    );
    t.case(
        "miss without default",
        post.x_lookup(
            "zzz",
            "c1",
            "c2",
            0,
            None,
            Some(0),
            Some(1),
            &f,
            &input,
            vfr,
        )
        .await,
    );
    t.case(
        "multi-column result",
        post.x_lookup(
            "P0010",
            "c1",
            "c2,c3",
            0,
            None,
            Some(0),
            Some(1),
            &f,
            &input,
            vfr,
        )
        .await,
    );

    t.section("LOOKUP");
    t.case(
        "hit",
        post.lookup("P0010", "c1", "c2", 0, &f, &input, vfr).await,
    );
    t.case(
        "miss",
        post.lookup("zzz", "c1", "c2", 0, &f, &input, vfr).await,
    );

    t.section("XMATCH");
    for (label, mm) in [
        ("exact", 0),
        ("next smallest", -1),
        ("next largest", 1),
        ("wildcard", 2),
    ] {
        let needle = if mm == 2 { "P001*" } else { "P0010" };
        t.case(
            &format!("match mode {label}"),
            post.x_match(needle, Some(mm), Some(1), "c1", 0, vfr).await,
        );
    }
    t.case(
        "search reverse",
        post.x_match("Lisboa", Some(0), Some(-1), "c2", 0, vfr)
            .await,
    );
    t.case(
        "miss",
        post.x_match("zzz", Some(0), Some(1), "c1", 0, vfr).await,
    );

    t.section("FILTER");
    let eq_lisboa = || {
        Condition::new(
            ConditionValue::new_columns("c2", false),
            "=",
            ConditionValue::new_value(Value::String("Lisboa".into()), false),
        )
    };
    let no_match = || {
        Condition::new(
            ConditionValue::new_columns("c2", false),
            "=",
            ConditionValue::new_value(Value::String("Atlantis".into()), false),
        )
    };
    let numeric_gt = || {
        Condition::new(
            ConditionValue::new_columns("c6", false),
            ">",
            ConditionValue::new_value(Value::F64(2.5), false),
        )
    };
    let wildcard = || {
        Condition::new(
            ConditionValue::new_columns("c1", false),
            "LIKE",
            ConditionValue::new_wildcard_value(
                Value::String("P001".into()),
                false,
                WildcardPosition::End,
            ),
        )
    };
    let compound = || {
        Condition::new(
            ConditionValue::new_condition(eq_lisboa()),
            "AND",
            ConditionValue::new_condition(numeric_gt()),
        )
    };

    t.case(
        "equality",
        post.filter(eq_lisboa(), "none", "c1", &f, &input, vfr)
            .await,
    );
    t.case(
        "equality multi-column",
        post.filter(eq_lisboa(), "none", "c1,c6", &f, &input, vfr)
            .await,
    );
    t.case(
        "no rows returns if_empty",
        post.filter(no_match(), "EMPTY", "c1", &f, &input, vfr)
            .await,
    );
    t.case(
        "numeric greater-than",
        post.filter(numeric_gt(), "none", "c1,c6", &f, &input, vfr)
            .await,
    );
    t.case(
        "LIKE wildcard",
        post.filter(wildcard(), "none", "c1", &f, &input, vfr).await,
    );
    t.case(
        "compound AND",
        post.filter(compound(), "none", "c1", &f, &input, vfr).await,
    );
    t.case_unordered(
        "across shards",
        shard
            .filter(eq_lisboa(), "none", "c1", &f, &input, vfr)
            .await,
    );

    t.section("SELECT ALL");
    t.case(
        "single column",
        accel.select_all("c1", &f, &input, vfr).await,
    );
    t.case(
        "multi column",
        accel.select_all("c1,c2", &f, &input, vfr).await,
    );
    t.case_unordered(
        "across shards",
        shard.select_all("c1", &f, &input, vfr).await,
    );

    t.section("MODIFIER PUSHDOWN");
    let m_order = SqlModifiers {
        order_by: Some(vec![(1, true)]),
        ..Default::default()
    };
    let m_distinct = SqlModifiers {
        distinct: true,
        ..Default::default()
    };
    let m_limit = SqlModifiers {
        limit_offset: Some((3, 0)),
        ..Default::default()
    };
    let m_offset = SqlModifiers {
        limit_offset: Some((3, 5)),
        ..Default::default()
    };

    t.case(
        "select_all ORDER BY desc",
        accel
            .select_all_with_modifiers("c1", &f, &input, vfr, &m_order)
            .await,
    );
    t.case_unordered(
        "select_all DISTINCT",
        post.select_all_with_modifiers("c2", &f, &input, vfr, &m_distinct)
            .await,
    );
    t.case(
        "select_all LIMIT",
        accel
            .select_all_with_modifiers("c1", &f, &input, vfr, &m_limit)
            .await,
    );
    t.case(
        "select_all LIMIT + OFFSET",
        accel
            .select_all_with_modifiers("c1", &f, &input, vfr, &m_offset)
            .await,
    );

    for (label, agg) in [
        ("SUM", SqlAggregate::Sum),
        ("COUNT", SqlAggregate::Count),
        ("COUNTA", SqlAggregate::CountA),
        ("AVERAGE", SqlAggregate::Average),
        ("MIN", SqlAggregate::Min),
        ("MAX", SqlAggregate::Max),
    ] {
        let m = SqlModifiers {
            aggregate: Some(agg),
            ..Default::default()
        };
        t.case(
            &format!("select_all aggregate {label}"),
            accel
                .select_all_with_modifiers("c1", &f, &input, vfr, &m)
                .await,
        );
    }

    t.case(
        "filter ORDER BY desc",
        post.filter_with_modifiers(eq_lisboa(), "none", "c1", &f, &input, vfr, &m_order)
            .await,
    );
    t.case_unordered(
        "filter DISTINCT",
        post.filter_with_modifiers(eq_lisboa(), "none", "c3", &f, &input, vfr, &m_distinct)
            .await,
    );
    t.case(
        "filter LIMIT",
        post.filter_with_modifiers(eq_lisboa(), "none", "c1", &f, &input, vfr, &m_limit)
            .await,
    );
    let m_sum = SqlModifiers {
        aggregate: Some(SqlAggregate::Sum),
        ..Default::default()
    };
    t.case(
        "filter aggregate SUM",
        post.filter_with_modifiers(eq_lisboa(), "none", "c6", &f, &input, vfr, &m_sum)
            .await,
    );
    t.case(
        "xlookup with modifiers",
        post.x_lookup_with_modifiers(
            "P0010",
            "c1",
            "c2",
            0,
            None,
            Some(0),
            Some(1),
            &f,
            &input,
            vfr,
            &m_limit,
        )
        .await,
    );

    t.section("WRITE OPERATIONS (read-only crate)");
    t.case("add_row", post.add_row(vec![], &f, &input, vfr).await);
    t.case(
        "update_row",
        post.update_row("1", vec![], &f, &input, vfr).await,
    );
    t.case("delete_row", post.delete_row("1", &f, &input, vfr).await);
    t.case("read_row", post.read_row("1", &f, &input, vfr).await);

    t.section("CACHING AND COALESCING");
    let first = render(
        post.v_lookup("P0001", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    let second = render(
        post.v_lookup("P0001", "c2", "c1", Some(false), &f, &input, vfr)
            .await,
    );
    t.note("repeated query is stable", first == second);
    t.note("repeated query value", &second);

    // Request coalescing: concurrent identical queries share one execution and must
    // all observe the same result.
    let post = Arc::new(post);
    let mut handles = Vec::new();
    for _ in 0..16 {
        let p = Arc::clone(&post);
        let vfc = vf.clone();
        handles.push(tokio::spawn(async move {
            let inp = Input::new("test", vfc.clone());
            let tf = TableFunctions::none();
            render(
                p.v_lookup("P0002", "c2", "c1", Some(false), &tf, &inp, &vfc)
                    .await,
            )
        }));
    }
    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.expect("join concurrent query"));
    }
    t.note(
        "16 concurrent queries agree",
        results.iter().all(|r| *r == results[0]),
    );
    t.note("coalesced value", &results[0]);

    let actual = t.0.trim().to_string();

    if std::env::var("SNAPSHOT").as_deref() == Ok("print") {
        println!("{actual}");
        return;
    }

    if actual != EXPECTED.trim() {
        // Show the first divergence rather than dumping two full transcripts.
        let mismatch = actual
            .lines()
            .zip(EXPECTED.trim().lines())
            .enumerate()
            .find(|(_, (a, e))| a != e)
            .map(|(i, (a, e))| format!("line {}:\n  actual:   {a}\n  expected: {e}", i + 1))
            .unwrap_or_else(|| {
                format!(
                    "line count differs: actual {} vs expected {}",
                    actual.lines().count(),
                    EXPECTED.trim().lines().count()
                )
            });
        panic!("table operation snapshot changed.\n{mismatch}\n\nRe-run with SNAPSHOT=print to emit the full transcript.");
    }
}

/// Expected transcript. Regenerate with `SNAPSHOT=print cargo test -- --nocapture`.
const EXPECTED: &str = r#"
=== VLOOKUP ===
numeric exact hit                            OK   F64(0.5)
numeric exact miss                           ERR  VLOOKUP: Search value 2.30 does not exist at column c2 for table accel
numeric range (<=)                           OK   F64(0.5)
numeric range default arg                    OK   F64(2.0)
numeric below minimum                        ERR  VLOOKUP: Search value 0.5 does not exist at column c2 for table accel
multi-column result                          OK   F64(2.25)
string exact                                 OK   String("M000")
string case-insensitive                      OK   String("Convertible")
string miss                                  ERR  VLOOKUP: Search value 'NONEXISTENT' does not exist at column c2 for table cars
sharded table                                OK   String("Porto")
rejects sql injection in column              ERR  Invalid SQL identifier: 'c2; DROP TABLE x'. Identifiers must contain only alphanumeric characters and underscores, and start with a letter or underscore.
rejects non-numeric on numeric col           ERR  Invalid numeric value: 'not-a-number'
escapes quote in string literal              ERR  VLOOKUP: Search value 'O''BRIEN' does not exist at column c2 for table cars

=== MATCH ===
exact (0)                                    OK   I32(10)
exact miss                                   ERR  MATCH: Search value zzzz does not exist for table post and match type 0
largest <= (1)                               OK   I32(10)
default match_type                           OK   I32(10)
smallest >= (-1)                             OK   I32(10)
numeric column                               OK   I32(195)
invalid match_type                           ERR  MATCH: The match_type must be -1, 0 or 1.  7 is not permitted
sharded table                                OK   I32(250)
horizontal row search                        OK   I32(2)

=== INDEX ===
row 1 column 2                               OK   String("Lisboa")
row 5 last column                            OK   F64(1.48)
column beyond end                            ERR  Index: Row 5 and column 7 position does not exist for table post
whole row                                    OK   Vec[n=6; first=String("P0001"); last=F64(0.0)]
row beyond end                               ERR  Index: Row 99999 and column 2 position does not exist for table post
row 0 (whole column)                         OK   Vec[n=500; first=String("Lisboa"); last=String("Braga")]
negative row                                 ERR  Index: Row -3 and column 2 position does not exist for table post
row spanning shards                          OK   String("Porto")

=== HLOOKUP ===
exact                                        OK   String("P0002")
range                                        OK   String("P0003")
miss                                         ERR  HLOOKUP: Search value nope does not exist at row 2 for table post

=== XLOOKUP ===
match mode exact (0)                         OK   String("Porto")
match mode next smallest (-1)                OK   String("Porto")
match mode next largest (1)                  OK   String("Porto")
wildcard mode (2)                            ERR  XSEARCH: Search value P001* does not exist for table post
search first                                 OK   String("P0001")
search reverse                               ERR  XSEARCH: Search value Lisboa does not exist for table post
binary first                                 OK   String("Porto")
binary last                                  OK   String("Porto")
if_not_found supplied                        OK   String("MISSING")
miss without default                         ERR  XSEARCH: Search value zzz does not exist for table post
multi-column result                          OK   Vec[n=2; first=String("Porto"); last=String("PORTO")]

=== LOOKUP ===
hit                                          OK   String("Porto")
miss                                         OK   String("Braga")

=== XMATCH ===
match mode exact                             OK   I32(10)
match mode next smallest                     OK   I32(10)
match mode next largest                      OK   I32(10)
match mode wildcard                          ERR  XMATCH: Search value P001* does not exist for table post
search reverse                               ERR  XMATCH: Search value Lisboa does not exist for table post
miss                                         ERR  XMATCH: Search value zzz does not exist for table post

=== FILTER ===
equality                                     OK   Area[rows=125, cols=1; first=String("P0001"); last=String("P0497")]
equality multi-column                        OK   Area[rows=125, cols=2; first=String("P0001"),F64(0.0); last=String("P0497"),F64(0.52)]
no rows returns if_empty                     OK   String("EMPTY")
numeric greater-than                         OK   Area[rows=82, cols=2; first=String("P0008"),F64(2.59); last=String("P0495"),F64(2.78)]
LIKE wildcard                                OK   Area[rows=10, cols=1; first=String("P0010"); last=String("P0019")]
compound AND                                 OK   Area[rows=24, cols=1; first=String("P0009"); last=String("P0397")]
across shards                                OK   Area[rows=75, cols=1; min=String("P0001"); max=String("P0297")]

=== SELECT ALL ===
single column                                OK   Area[rows=81, cols=1; first=F64(2.0); last=F64(22.0)]
multi column                                 OK   Area[rows=81, cols=2; first=F64(2.0),F64(0.0); last=F64(22.0),F64(40.0)]
across shards                                OK   Area[rows=300, cols=1; min=String("P0001"); max=String("P0300")]

=== MODIFIER PUSHDOWN ===
select_all ORDER BY desc                     OK   Area[rows=81, cols=1; first=F64(22.0); last=F64(2.0)]
select_all DISTINCT                          OK   Area[rows=4, cols=1; min=String("Braga"); max=String("Porto")]
select_all LIMIT                             OK   Area[rows=3, cols=1; first=F64(2.0); last=F64(2.5)]
select_all LIMIT + OFFSET                    OK   Area[rows=3, cols=1; first=F64(3.25); last=F64(3.75)]
select_all aggregate SUM                     OK   F64(972.0)
select_all aggregate COUNT                   OK   F64(81.0)
select_all aggregate COUNTA                  OK   F64(81.0)
select_all aggregate AVERAGE                 OK   F64(12.0)
select_all aggregate MIN                     OK   F64(2.0)
select_all aggregate MAX                     OK   F64(22.0)
filter ORDER BY desc                         OK   Area[rows=125, cols=1; first=String("P0497"); last=String("P0001")]
filter DISTINCT                              OK   Area[rows=1, cols=1; min=String("LISBOA"); max=String("LISBOA")]
filter LIMIT                                 OK   Area[rows=3, cols=1; first=String("P0001"); last=String("P0009")]
filter aggregate SUM                         OK   F64(196.0)
xlookup with modifiers                       OK   String("Porto")

=== WRITE OPERATIONS (read-only crate) ===
add_row                                      ERR  ADDROW: Not implemented for read only tables
update_row                                   ERR  UPDATEROW: Not implemented for read only tables
delete_row                                   ERR  DELETEROW: Not implemented for read only tables
read_row                                     ERR  READROW: Not implemented for read only tables

=== CACHING AND COALESCING ===
repeated query is stable                     true
repeated query value                         OK   String("Lisboa")
16 concurrent queries agree                  true
coalesced value                              OK   String("Porto")
"#;
