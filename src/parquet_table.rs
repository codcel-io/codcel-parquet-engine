// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT and LICENSE-APACHE in the project root.

use crate::sql_cache::SqlCache;
use async_trait::async_trait;
use codcel_calculation_engine::input::Input;
use codcel_calculation_engine::value::Value;
use codcel_calculation_engine::value_format::ValueFormat;
use codcel_table_engine::codcel_table::CodcelTable;
use codcel_table_engine::column_type::ColumnType;
use codcel_table_engine::condition::Condition;
use codcel_table_engine::searchable::{
    find_exact_position, find_largest_position, find_smallest_position, Searchable,
};
use codcel_table_engine::sql_modifiers::{SqlAggregate, SqlModifiers};
use codcel_table_engine::table_constants::{
    X_MATCH_MODE_EXACT, X_MATCH_MODE_EXACT_NEXT_LARGEST, X_MATCH_MODE_EXACT_NEXT_SMALLEST,
    X_MATCH_MODE_WILDCARD, X_SEARCH_MODE_BINARY_FIRST, X_SEARCH_MODE_BINARY_LAST,
    X_SEARCH_MODE_FIRST, X_SEARCH_MODE_REVERSE,
};
use codcel_table_engine::table_functions::TableFunctions;
use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, StringViewArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::basic::LogicalType;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::RwLock;

/// Matches `^[a-zA-Z_][a-zA-Z0-9_]*$` without a regex, so there is no fallible
/// initialisation to unwrap in a `static`.
fn is_sql_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Validates that a string is a safe SQL identifier (column name, table name, etc.)
/// Returns Ok(()) if valid, Err with message if invalid.
/// Valid identifiers contain only alphanumeric characters and underscores,
/// and must start with a letter or underscore.
fn validate_sql_identifier(identifier: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if identifier.is_empty() {
        return Err("SQL identifier cannot be empty".into());
    }
    if !is_sql_identifier(identifier) {
        return Err(format!("Invalid SQL identifier: '{}'. Identifiers must contain only alphanumeric characters and underscores, and start with a letter or underscore.", identifier).into());
    }
    Ok(())
}

/// Validates a comma-separated list of column identifiers
fn validate_column_list(columns: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    for col in columns.split(',') {
        let col = col.trim();
        if col.is_empty() {
            continue;
        }
        // Handle expressions like UPPER(column_name)
        if col.starts_with("UPPER(") && col.ends_with(')') {
            let inner = &col[6..col.len() - 1];
            validate_sql_identifier(inner)?;
        } else {
            validate_sql_identifier(col)?;
        }
    }
    Ok(())
}

/// Escapes a string value for use in SQL queries (prevents SQL injection in string literals)
fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// Build SQL clause fragments from SqlModifiers for Parquet/DataFusion queries.
/// Returns (select_prefix, order_clause, limit_clause).
fn build_parquet_modifier_clauses(
    modifiers: &SqlModifiers,
    col_list: &[String],
) -> (String, String, String) {
    let select_prefix = if modifiers.distinct {
        "DISTINCT ".to_string()
    } else {
        String::new()
    };

    let order_clause = if let Some(ref order) = modifiers.order_by {
        let parts: Vec<String> = order
            .iter()
            .map(|&(idx, desc): &(usize, bool)| {
                let col = if idx > 0 && idx <= col_list.len() {
                    &col_list[idx - 1]
                } else {
                    &col_list[0]
                };
                format!("{} {}", col, if desc { "DESC" } else { "ASC" })
            })
            .collect();
        format!(" ORDER BY {}", parts.join(", "))
    } else {
        String::new()
    };

    let limit_clause = if let Some((limit, offset)) = modifiers.limit_offset {
        format!(" LIMIT {} OFFSET {}", limit, offset)
    } else {
        String::new()
    };

    (select_prefix, order_clause, limit_clause)
}

/// A 2D grid of values representing query results.
///
/// The outer `Vec` represents rows, and each inner `Vec` represents the columns
/// within that row. This is the return type for area-based queries like `filter()`
/// and `select_all()`.
///
/// For example, a result with 3 rows and 2 columns would be structured as:
/// ```text
/// [
///     [row0_col0, row0_col1],  // Row 0
///     [row1_col0, row1_col1],  // Row 1
///     [row2_col0, row2_col1],  // Row 2
/// ]
/// ```
pub type RowColumnValues = Vec<Vec<Value>>;

/// Extract a single value at a given index from an Arrow array column
fn extract_value_at_index(column: &dyn Array, index: usize) -> Option<Value> {
    if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
        Some(Value::String(String::from(array.value(index))))
    } else if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        Some(Value::String(String::from(array.value(index))))
    } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
        Some(Value::I32(array.value(index)))
    } else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
        Some(Value::F64(array.value(index)))
    } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        Some(Value::F64(array.value(index) as f64))
    } else if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
        Some(Value::F64(array.value(index) as f64))
    } else {
        column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|array| Value::I32(array.value(index) as i32))
    }
}

/// Push all values from an Arrow array column into a Vec
///
/// For string arrays, this uses direct indexing with null bitmap check to avoid
/// Option unwrapping overhead when the array has no nulls.
fn push_all_values(column: &dyn Array, values: &mut Vec<Value>) {
    if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
        // Check if array has no nulls - use direct indexing for better performance
        if array.null_count() == 0 {
            for i in 0..array.len() {
                values.push(Value::String(String::from(array.value(i))));
            }
        } else {
            for value in array.iter().flatten() {
                values.push(Value::String(String::from(value)));
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        if array.null_count() == 0 {
            for i in 0..array.len() {
                values.push(Value::String(String::from(array.value(i))));
            }
        } else {
            for value in array.iter().flatten() {
                values.push(Value::String(String::from(value)));
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
        if array.null_count() == 0 {
            values.extend(array.values().iter().map(|&v| Value::I32(v)));
        } else {
            for value in array.iter().flatten() {
                values.push(Value::I32(value));
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
        if array.null_count() == 0 {
            values.extend(array.values().iter().map(|&v| Value::F64(v)));
        } else {
            for value in array.iter().flatten() {
                values.push(Value::F64(value));
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<UInt32Array>() {
        if array.null_count() == 0 {
            values.extend(array.values().iter().map(|&v| Value::I32(v as i32)));
        } else {
            for value in array.iter().flatten() {
                values.push(Value::I32(value as i32));
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        if array.null_count() == 0 {
            values.extend(array.values().iter().map(|&v| Value::F64(v as f64)));
        } else {
            for value in array.iter().flatten() {
                values.push(Value::F64(value as f64));
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
        if array.null_count() == 0 {
            values.extend(array.values().iter().map(|&v| Value::F64(v as f64)));
        } else {
            for value in array.iter().flatten() {
                values.push(Value::F64(value as f64));
            }
        }
    }
}

/// Push values from an Arrow array column into a transposed row structure
///
/// `row_offset` is the index of the row this column's first value belongs to. A query
/// result may arrive as several `RecordBatch`es (one per file of a sharded table, or one
/// per output partition of an aggregate), and each batch fills the block of rows that
/// was reserved for it. Writing from index 0 for every batch would append later batches'
/// values onto the first batch's rows instead.
///
/// For string arrays, this uses direct indexing with null bitmap check to avoid
/// Option unwrapping overhead when the array has no nulls.
fn push_values_transposed(
    column: &dyn Array,
    values_transposed: &mut RowColumnValues,
    row_offset: usize,
) {
    let rows = &mut values_transposed[row_offset..];

    if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
        if array.null_count() == 0 {
            for (row_index, row) in rows.iter_mut().enumerate().take(array.len()) {
                row.push(Value::String(String::from(array.value(row_index))));
            }
        } else {
            for (row_index, value) in array.iter().enumerate() {
                if let Some(v) = value {
                    rows[row_index].push(Value::String(String::from(v)));
                }
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        if array.null_count() == 0 {
            for (row_index, row) in rows.iter_mut().enumerate().take(array.len()) {
                row.push(Value::String(String::from(array.value(row_index))));
            }
        } else {
            for (row_index, value) in array.iter().enumerate() {
                if let Some(v) = value {
                    rows[row_index].push(Value::String(String::from(v)));
                }
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
        if array.null_count() == 0 {
            for (row_index, &v) in array.values().iter().enumerate() {
                rows[row_index].push(Value::I32(v));
            }
        } else {
            for (row_index, value) in array.iter().enumerate() {
                if let Some(v) = value {
                    rows[row_index].push(Value::I32(v));
                }
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
        if array.null_count() == 0 {
            for (row_index, &v) in array.values().iter().enumerate() {
                rows[row_index].push(Value::F64(v));
            }
        } else {
            for (row_index, value) in array.iter().enumerate() {
                if let Some(v) = value {
                    rows[row_index].push(Value::F64(v));
                }
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<UInt32Array>() {
        if array.null_count() == 0 {
            for (row_index, &v) in array.values().iter().enumerate() {
                rows[row_index].push(Value::I32(v as i32));
            }
        } else {
            for (row_index, value) in array.iter().enumerate() {
                if let Some(v) = value {
                    rows[row_index].push(Value::I32(v as i32));
                }
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        if array.null_count() == 0 {
            for (row_index, &v) in array.values().iter().enumerate() {
                rows[row_index].push(Value::F64(v as f64));
            }
        } else {
            for (row_index, value) in array.iter().enumerate() {
                if let Some(v) = value {
                    rows[row_index].push(Value::F64(v as f64));
                }
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
        if array.null_count() == 0 {
            for (row_index, &v) in array.values().iter().enumerate() {
                rows[row_index].push(Value::F64(v as f64));
            }
        } else {
            for (row_index, value) in array.iter().enumerate() {
                if let Some(v) = value {
                    rows[row_index].push(Value::F64(v as f64));
                }
            }
        }
    } else if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
        if array.null_count() == 0 {
            for (row_index, row) in rows.iter_mut().enumerate().take(array.len()) {
                row.push(Value::Bool(array.value(row_index)));
            }
        } else {
            for (row_index, value) in array.iter().enumerate() {
                if let Some(v) = value {
                    rows[row_index].push(Value::Bool(v));
                }
            }
        }
    }
}

/// Convert from datafusion's LogicalType to our abstract ColumnType
fn logical_type_to_column_type(lt: &Option<LogicalType>) -> ColumnType {
    match lt {
        Some(LogicalType::String) => ColumnType::Text,
        Some(LogicalType::Integer { bit_width, .. }) => {
            if *bit_width <= 32 {
                ColumnType::Integer
            } else {
                ColumnType::BigInt
            }
        }
        Some(LogicalType::Decimal { .. }) => ColumnType::Double,
        Some(LogicalType::Float16) => ColumnType::Float,
        Some(LogicalType::Date) => ColumnType::Date,
        Some(LogicalType::Timestamp { .. }) => ColumnType::Timestamp,
        Some(LogicalType::Time { .. }) => ColumnType::Timestamp,
        Some(LogicalType::Bson) | Some(LogicalType::Json) => ColumnType::Binary,
        Some(LogicalType::Uuid) => ColumnType::Text,
        Some(LogicalType::Enum) => ColumnType::Text,
        _ => ColumnType::Double, // Default to double for unknown/unspecified types
    }
}

/// A read-only table backed by Parquet files with Excel-like lookup operations.
///
/// `ParquetTable` implements the `CodcelTable` trait, providing familiar spreadsheet
/// operations (VLOOKUP, XLOOKUP, INDEX, MATCH, FILTER, etc.) on Parquet data files.
/// It uses DataFusion for SQL query execution with automatic result caching.
///
/// # Features
///
/// - **Excel-compatible lookups**: VLOOKUP, HLOOKUP, XLOOKUP, LOOKUP
/// - **Position matching**: MATCH, XMATCH with multiple match modes
/// - **Data retrieval**: INDEX for direct cell access, FILTER for conditional selection
/// - **Sharded file support**: Automatically handles tables split across multiple files
///   (e.g., `table_xyz0.parquet`, `table_xyz1.parquet`, ...)
/// - **Query caching**: Results are cached to improve performance on repeated queries
///
/// # Read-Only
///
/// This implementation is read-only. Write operations (`add_row`, `update_row`,
/// `delete_row`, `read_row`) return errors indicating the operation is not supported.
///
/// # Column Naming
///
/// Columns are referenced by name (e.g., `c0`, `c1`, `c2`). The first column `c0`
/// typically contains row identifiers.
pub struct ParquetTable {
    name: String,
    filename: String,
    column_types: HashMap<String, Option<LogicalType>>,
    abstract_column_types: OnceLock<HashMap<String, ColumnType>>,
    sql_cache: Arc<RwLock<SqlCache>>,
    column_count: i64,
    row_count: i64,
}

impl ParquetTable {
    /// Initializes a new `ParquetTable` from a Parquet file.
    ///
    /// This constructor reads the Parquet file metadata to determine column types
    /// and row count, sets up the SQL query cache, and starts the cache cleanup task.
    ///
    /// # Arguments
    ///
    /// * `filename` - Path to the Parquet file. For sharded tables, provide the path
    ///   to any shard; the `_xyz*.parquet` pattern will be used to find all shards.
    /// * `file_shortname` - Short name for the table, typically the filename without path.
    ///   The `.parquet` extension is automatically stripped to form the table name.
    ///
    /// # Returns
    ///
    /// A fully initialized `ParquetTable` ready for query operations.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Parquet file cannot be opened or read
    /// - The file is not a valid Parquet format
    /// - File metadata cannot be extracted
    pub async fn init(
        filename: String,
        file_shortname: &str,
    ) -> Result<ParquetTable, Box<dyn Error + Send + Sync>> {
        let name = file_shortname.replace(".parquet", "");

        let column_types: HashMap<String, Option<LogicalType>> = HashMap::new();

        // Cleanup cache every 5 mins, start the cleanup task
        let sql_cache = SqlCache::new(300);
        sql_cache.start_cleanup_task();

        let mut parquet_table = ParquetTable {
            name,
            filename: filename.replace(".parquet", "_xyz*.parquet"),
            column_types,
            abstract_column_types: OnceLock::new(),
            sql_cache: Arc::new(RwLock::new(sql_cache)),
            column_count: 0,
            row_count: 0,
        };

        parquet_table.set_column_types()?;
        parquet_table.row_count = parquet_table.compute_row_count()?;

        Ok(parquet_table)
    }

    /// Get column types as the abstract ColumnType for use with Condition
    fn get_abstract_column_types(&self) -> &HashMap<String, ColumnType> {
        self.abstract_column_types.get_or_init(|| {
            self.column_types
                .iter()
                .map(|(k, v)| (k.clone(), logical_type_to_column_type(v)))
                .collect()
        })
    }

    fn search_value_column(
        &self,
        search_value: &str,
        to_upper: bool,
        decimal_separator: &str,
        search_column: &str,
    ) -> Result<(String, String), Box<dyn Error + Send + Sync>> {
        // Validate the search column identifier
        validate_sql_identifier(search_column)?;

        if let Some(column_type) = self.column_types.get(search_column) {
            return match column_type {
                Some(LogicalType::String) | Some(LogicalType::Enum) | Some(LogicalType::Uuid) => {
                    if to_upper {
                        // Escape the string value to prevent SQL injection
                        let escaped_value = escape_sql_string(&search_value.to_uppercase());
                        Ok((
                            format!("'{}'", escaped_value),
                            format!("UPPER({search_column})"),
                        ))
                    } else {
                        let escaped_value = escape_sql_string(search_value);
                        Ok((format!("'{}'", escaped_value), search_column.to_string()))
                    }
                }
                _ => {
                    // We assume it is a number - validate it's actually numeric
                    let search_value_f64 = search_value.replace(decimal_separator, ".");
                    // Verify it's a valid number to prevent injection
                    if search_value_f64.parse::<f64>().is_err() {
                        return Err(format!("Invalid numeric value: '{}'", search_value).into());
                    }
                    Ok((search_value_f64, search_column.to_string()))
                }
            };
        };

        // If column type is unknown, escape as string to be safe
        let escaped_value = escape_sql_string(search_value);
        Ok((format!("'{}'", escaped_value), search_column.to_string()))
    }

    fn open_file(&self) -> Result<File, Box<dyn Error + Send + Sync>> {
        let filename = &self.filename.replace("_xyz*.parquet", "_xyz0.parquet");
        match File::open(Path::new(filename)) {
            Ok(file) => Ok(file),
            Err(_) => Err(format!("Couldn't open table {}", self.name).into()),
        }
    }

    fn count_rows(&self) -> i64 {
        self.row_count
    }

    fn compute_row_count(&self) -> Result<i64, Box<dyn Error + Send + Sync>> {
        let mut total_rows = 0;
        let mut index = 0;

        loop {
            // Construct the file path with incremental index
            let filename = &self
                .filename
                .replace("_xyz*.parquet", &format!("_xyz{index:}.parquet"));
            let path = Path::new(&filename);

            // Check if the file exists
            if !path.exists() {
                break;
            }

            // Open the Parquet file
            let file = File::open(path)?;
            let reader = SerializedFileReader::new(file)?;

            // Get metadata and count rows
            let parquet_metadata = reader.metadata();
            let rows = parquet_metadata.file_metadata().num_rows();

            // Add rows to the total count
            total_rows += rows;

            // Increment the file index
            index += 1;
        }

        Ok(total_rows)
    }

    fn set_column_types(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let file = self.open_file()?;
        let reader = SerializedFileReader::new(file)?;
        let parquet_metadata = reader.metadata();
        let fields = parquet_metadata.file_metadata().schema().get_fields();

        self.column_count = fields.len() as i64;

        self.column_types = fields
            .iter()
            .map(|field| {
                let basic_info = field.get_basic_info();
                (
                    basic_info.name().to_string(),
                    basic_info.logical_type_ref().cloned(),
                )
            })
            .collect::<HashMap<_, _>>();

        Ok(())
    }

    async fn sql_query(
        &self,
        name: &str,
        filename: &str,
        sql_query: &str,
    ) -> Result<Option<Arc<Vec<RecordBatch>>>, Box<dyn Error + Send + Sync>> {
        // Use read lock - SqlCache uses interior mutability for concurrent access
        let cache = self.sql_cache.read().await;
        cache.sql_query(name, filename, sql_query).await
    }

    async fn sql_query_name_response(
        &self,
        name: &str,
        filename: &str,
        sql_query: &str,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let batches_option = self.sql_query(name, filename, sql_query).await?;

        // Extract the value (assuming there is at least one row and one column in the result)
        if let Some(batches) = batches_option {
            if let Some(batch) = batches.first() {
                if let Some(column) = batch.columns().first() {
                    if let Some(value) = extract_value_at_index(column.as_ref(), 0) {
                        return Ok(value);
                    }
                }
            }
        }

        // TODO: PERHAPS DO NOT RAISE AN ERROR????
        // TODO, PERHAPS RAISE AN ERROR HERE
        //    Ok("".to_string())
        Err("No SQL response found.".into())
    }

    async fn sql_query_name_responses(
        &self,
        name: &str,
        filename: &str,
        sql_query: &str,
    ) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        let batches_option = self.sql_query(name, filename, sql_query).await?;

        let estimated_capacity = batches_option
            .as_ref()
            .map(|b| b.iter().map(|batch| batch.num_rows()).sum())
            .unwrap_or(0);
        let mut values: Vec<Value> = Vec::with_capacity(estimated_capacity);

        // Extract the values from all batches
        if let Some(batches) = batches_option {
            for batch in batches.iter() {
                for column in batch.columns() {
                    push_all_values(column.as_ref(), &mut values);
                }
            }
        }

        if values.is_empty() {
            Err("No SQL response found.".into())
        } else {
            Ok(values)
        }
    }

    async fn sql_query_name_area_responses(
        &self,
        name: &str,
        filename: &str,
        sql_query: &str,
    ) -> Result<RowColumnValues, Box<dyn Error + Send + Sync>> {
        let batches_option = self.sql_query(name, filename, sql_query).await?;

        let mut values_transposed: RowColumnValues = vec![];

        if let Some(batches) = batches_option {
            for batch in batches.iter() {
                let column_count = batch.num_columns();
                let row_count = batch.num_rows();

                // Each batch fills its own block of rows, appended after the rows already
                // written by earlier batches.
                let row_offset = values_transposed.len();
                values_transposed
                    .resize_with(row_offset + row_count, || Vec::with_capacity(column_count));

                for column in batch.columns().iter() {
                    push_values_transposed(column.as_ref(), &mut values_transposed, row_offset);
                }
            }
        }

        if values_transposed.is_empty() {
            Err("No SQL response found.".into())
        } else {
            Ok(values_transposed)
        }
    }

    async fn sql_query_response(
        &self,
        sql_query: &str,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        self.sql_query_name_response(&self.name, &self.filename, sql_query)
            .await
    }

    async fn sql_query_responses(
        &self,
        sql_query: &str,
    ) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        self.sql_query_name_responses(&self.name, &self.filename, sql_query)
            .await
    }

    async fn sql_query_area_responses(
        &self,
        sql_query: &str,
    ) -> Result<RowColumnValues, Box<dyn Error + Send + Sync>> {
        self.sql_query_name_area_responses(&self.name, &self.filename, sql_query)
            .await
    }

    async fn run_functions(
        value: Value,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if let Ok(val) = value.string(value_format) {
            if let Some(stripped) = val.strip_prefix("*P*") {
                // Parameterized table function: *P*template_name:const1:const2:...
                if let Some(ref param_map) = table_functions.param_functions {
                    let mut parts = stripped.splitn(2, ':');
                    let template_name = parts.next().unwrap_or("");
                    let constants_str = parts.next().unwrap_or("");
                    let params: Vec<Value> = constants_str
                        .split(':')
                        .filter(|s| !s.is_empty())
                        .map(|s| {
                            // Try i32 first to preserve integer types (e.g., "1" → I32(1))
                            if let Ok(i) = s.parse::<i32>() {
                                Value::I32(i)
                            } else if let Ok(f) = s.parse::<f64>() {
                                Value::F64(f)
                            } else {
                                Value::String(s.to_string())
                            }
                        })
                        .collect();
                    if let Some(fun) = param_map.get(template_name) {
                        if let Ok(result) = fun(Arc::new(input.clone()), params).await {
                            return Ok(result);
                        }
                    }
                }
            } else if let Some(stripped) = val.strip_prefix("*F*") {
                if let Some(ref table_functions_map) = table_functions.functions {
                    if let Some(fun) = table_functions_map.get(stripped) {
                        // TODO: Potential performance problem (Arc::new(input.clone()))
                        if let Ok(result) = fun(Arc::new(input.clone())).await {
                            return Ok(result);
                        }
                    }
                }
            }
        }
        Ok(value)
    }

    fn generate_column_string(&self) -> String {
        let mut result = String::with_capacity((self.column_count as usize) * 4);
        for i in 1..self.column_count {
            if i > 1 {
                result.push(',');
            }
            result.push('c');
            result.push_str(&i.to_string());
        }
        result
    }

    fn x_search_query(
        &self,
        lookup_value: &str,
        search_column: &str,
        columns: &str,
        match_mode: Option<i32>,
        search_mode: Option<i32>,
        value_format: &ValueFormat,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        // Validate all column identifiers
        validate_column_list(columns)?;
        validate_column_list(search_column)?;

        let match_mode = if let Some(mode) = match_mode {
            mode
        } else {
            X_MATCH_MODE_EXACT
        };

        let search_mode = if let Some(mode) = search_mode {
            mode
        } else {
            X_SEARCH_MODE_FIRST
        };

        let (lookup_value, search_column) = self.search_value_column(
            lookup_value,
            true,
            &value_format.decimal_separator,
            search_column,
        )?;

        // TODO: This SQL logic needs better testing
        let sql_query = match match_mode {
            X_MATCH_MODE_EXACT => {
                match search_mode {
                    X_SEARCH_MODE_FIRST => {
                        format!("SELECT {columns} FROM {} WHERE {search_column} = {lookup_value} LIMIT 1", self.name)
                    }
                    X_SEARCH_MODE_REVERSE => {
                        format!("SELECT {columns} FROM {} WHERE {search_column} = (SELECT MAX(c1) FROM {} WHERE {search_column} = {lookup_value}) LIMIT 1;", self.name, self.name)
                    }
                    X_SEARCH_MODE_BINARY_FIRST => {
                        format!("SELECT {columns} FROM {} WHERE {search_column} = {lookup_value} ORDER BY {search_column} ASC LIMIT 1", self.name)
                    }
                    X_SEARCH_MODE_BINARY_LAST => {
                        format!("SELECT {columns} FROM {} WHERE {search_column} = {lookup_value} ORDER BY {search_column} DESC LIMIT 1", self.name)
                    }
                    _ => "".to_string(),
                }
            }
            X_MATCH_MODE_EXACT_NEXT_LARGEST => {
                match search_mode {
                    X_SEARCH_MODE_FIRST => {
                        format!("SELECT {columns} FROM {} WHERE {search_column} >= {lookup_value} LIMIT 1", self.name)
                    }
                    X_SEARCH_MODE_REVERSE => {
                        format!("SELECT {columns} FROM {} WHERE {search_column} = (SELECT MIN(c1) FROM {} WHERE {search_column} >= {lookup_value}) LIMIT 1;", self.name, self.name)
                    }
                    X_SEARCH_MODE_BINARY_FIRST => {
                        format!("SELECT {columns} FROM {} WHERE {search_column} >= {lookup_value} ORDER BY {search_column} ASC LIMIT 1", self.name)
                    }
                    X_SEARCH_MODE_BINARY_LAST => {
                        format!("SELECT {columns} FROM {} WHERE {search_column} >= {lookup_value} ORDER BY {search_column} DESC LIMIT 1", self.name)
                    }
                    _ => "".to_string(),
                }
            }
            X_MATCH_MODE_EXACT_NEXT_SMALLEST => match search_mode {
                X_SEARCH_MODE_FIRST => {
                    format!("SELECT {columns} FROM {} WHERE {search_column} = (SELECT MAX({search_column}) FROM {} WHERE {search_column} <= {lookup_value}) LIMIT 1;", self.name, self.name)
                }
                X_SEARCH_MODE_REVERSE => {
                    format!("SELECT {columns} FROM {} WHERE {search_column} = (SELECT MAX({search_column}) FROM {} WHERE {search_column} <= {lookup_value}) LIMIT 1;", self.name, self.name)
                }
                X_SEARCH_MODE_BINARY_FIRST => {
                    format!("SELECT {columns} FROM {} WHERE {search_column} = (SELECT MAX({search_column}) FROM {} WHERE {search_column} <= {lookup_value}) ORDER BY {search_column} ASC LIMIT 1", self.name, self.name)
                }
                X_SEARCH_MODE_BINARY_LAST => {
                    format!("SELECT {columns} FROM {} WHERE {search_column} <= {lookup_value} ORDER BY {search_column} DESC LIMIT 1", self.name)
                }
                _ => "".to_string(),
            },
            X_MATCH_MODE_WILDCARD => "".to_string(), /*match search_mode {
            // TODO
            _ => "".to_string()
            }*/
            _ => "".to_string(),
        };

        Ok(sql_query)
    }
}

fn search_value_pure(search_value: &str, to_upper: bool, decimal_separator: &str) -> String {
    let search_value_f64 = search_value.replace(decimal_separator, ".");

    if search_value_f64.parse::<f64>().is_ok() {
        search_value_f64.to_string()
    } else if to_upper {
        search_value.to_uppercase()
    } else {
        search_value.to_string()
    }
}

async fn process_area_results(
    value_result: Result<RowColumnValues, Box<dyn Error + Send + Sync>>,
    table_functions: &TableFunctions,
    input: &Input,
    value_format: &ValueFormat,
) -> Option<Value> {
    let mut result: RowColumnValues = vec![];
    if let Ok(vals) = value_result {
        for inside in vals {
            let mut values: Vec<Value> = vec![];
            for val in inside {
                if let Ok(result) =
                    ParquetTable::run_functions(val, table_functions, input, value_format).await
                {
                    values.push(result);
                }
            }
            result.push(values);
        }
    }

    if !result.is_empty() {
        return Some(Value::AreaValue(result));
    }

    None
}

async fn process_results(
    value_result: Result<Vec<Value>, Box<dyn Error + Send + Sync>>,
    table_functions: &TableFunctions,
    input: &Input,
    value_format: &ValueFormat,
) -> Option<Value> {
    let mut values: Vec<Value> = vec![];

    if let Ok(vals) = value_result {
        for val in vals {
            if let Ok(result) =
                ParquetTable::run_functions(val, table_functions, input, value_format).await
            {
                values.push(result);
            }
        }
    }

    if !values.is_empty() {
        return Some(Value::VecValue(values));
    }

    None
}

async fn process_result(
    value_result: Result<Value, Box<dyn Error + Send + Sync>>,
    table_functions: &TableFunctions,
    input: &Input,
    value_format: &ValueFormat,
) -> Option<Value> {
    if let Ok(value) = value_result {
        if let Ok(result) =
            ParquetTable::run_functions(value, table_functions, input, value_format).await
        {
            return Some(result);
        }
    }
    None
}

fn process_horizontal_match_position(
    collection: &mut dyn Searchable,
    match_value: &str,
    match_type: i32,
    search_mode: i32,
) -> Result<Option<Value>, Box<dyn Error + Send + Sync>> {
    match search_mode {
        -1 => {
            collection.reverse_order();
        }
        2 => {
            collection.sort_ascending();
        }
        -2 => {
            collection.sort_descending();
        }
        _ => {}
    }

    let pos = match match_type {
        -1 => find_smallest_position(collection, match_value),
        0 => find_exact_position(collection, match_value),
        1 => find_largest_position(collection, match_value),
        2 => {
            // TODO Wildcard match to implement for XMATCH
            None
        }
        _ => None,
    };

    if let Some(pos) = pos {
        return Ok(Some(Value::I32((pos + 1) as i32)));
    }

    Err("MATCH: Position not found".into())
}

fn process_horizontal_match_results(
    match_value: &str,
    match_type: i32,
    search_mode: Option<i32>,
    values: Vec<Value>,
) -> Result<Option<Value>, Box<dyn Error + Send + Sync>> {
    let search_mode = search_mode.unwrap_or(1);

    // Check the types of all elements in a single pass
    let (mut has_f64, mut has_i32, mut has_string) = (false, false, false);
    for v in &values {
        match v {
            Value::F64(_) => has_f64 = true,
            Value::I32(_) => has_i32 = true,
            Value::String(_) => has_string = true,
            _ => {}
        }
    }
    let all_f64 = has_f64 && !has_i32 && !has_string;
    let all_i32 = has_i32 && !has_f64 && !has_string;
    let mixed_f64_i32 = (has_f64 || has_i32) && !has_string;
    let contains_string = has_string;

    if all_f64 {
        // If all elements are f64, create Vec<f64>
        let mut vals: Vec<f64> = values
            .into_iter()
            .map(|v| match v {
                Value::F64(value) => value,
                _ => unreachable!(),
            })
            .collect();
        return process_horizontal_match_position(
            &mut vals as &mut dyn Searchable,
            match_value,
            match_type,
            search_mode,
        );
    } else if all_i32 {
        // If all elements are i32, create Vec<i32>
        let mut vals: Vec<i32> = values
            .into_iter()
            .map(|v| match v {
                Value::I32(value) => value,
                _ => unreachable!(),
            })
            .collect();
        return process_horizontal_match_position(
            &mut vals as &mut dyn Searchable,
            match_value,
            match_type,
            search_mode,
        );
    } else if mixed_f64_i32 {
        // If mixed f64 and i32, create Vec<f64> with all values as f64
        let mut vals: Vec<f64> = values
            .into_iter()
            .map(|v| match v {
                Value::F64(value) => value,
                Value::I32(value) => value as f64,
                _ => unreachable!(),
            })
            .collect();
        return process_horizontal_match_position(
            &mut vals as &mut dyn Searchable,
            match_value,
            match_type,
            search_mode,
        );
    } else if contains_string {
        // If it contains any String, create Vec<String> with all values
        let mut vals: Vec<String> = values
            .into_iter()
            .map(|v| match v {
                Value::F64(value) => value.to_string(),
                Value::I32(value) => value.to_string(),
                Value::String(value) => value,
                _ => unreachable!(),
            })
            .collect();
        return process_horizontal_match_position(
            &mut vals as &mut dyn Searchable,
            match_value,
            match_type,
            search_mode,
        );
    }

    Ok(None)
}

#[async_trait]
impl CodcelTable for ParquetTable {
    /// Performs a vertical lookup (VLOOKUP) on the table.
    ///
    /// Searches for a value in the first column and returns a value from the same row
    /// in a specified column. This is equivalent to Excel's VLOOKUP function.
    ///
    /// # Arguments
    ///
    /// * `lookup_value` - The value to search for in the search column
    /// * `result_column_index` - The column(s) to return values from (e.g., `"c2"` or `"c2,c3"`)
    /// * `search_column_index` - The column to search in (typically `"c1"`)
    /// * `range` - If `Some(true)` (default), finds closest match <= lookup_value;
    ///   if `Some(false)`, requires exact match
    /// * `table_functions` - Optional functions to apply to the result value
    /// * `input` - Input context for function evaluation
    /// * `value_format` - Formatting options including decimal separator
    ///
    /// # Returns
    ///
    /// The value from the result column in the matching row.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The lookup value is not found in the search column
    /// - Column identifiers are invalid (SQL injection protection)
    /// - The query execution fails
    #[allow(clippy::too_many_arguments)]
    async fn v_lookup(
        &self,
        lookup_value: &str,
        result_column_index: &str,
        search_column_index: &str,
        range: Option<bool>,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Validate column identifiers - result_column_index may be a comma-separated list
        validate_column_list(result_column_index)?;
        validate_column_list(search_column_index)?;

        let range_lookup = range.unwrap_or(true);

        let (lookup_value, lookup_value_column) =
            self.search_value_column(lookup_value, true, &value_format.decimal_separator, "c1")?;
        let sql_query = if range_lookup {
            format!("SELECT {result_column_index} FROM {} WHERE {lookup_value_column} <= {lookup_value} ORDER BY {search_column_index} DESC LIMIT 1", self.name)
        } else {
            format!("SELECT {result_column_index} FROM {} WHERE {lookup_value_column} = {lookup_value} LIMIT 1", self.name)
        };

        let value_result = self.sql_query_response(&sql_query).await;

        if let Some(result) =
            process_result(value_result, table_functions, input, value_format).await
        {
            return Ok(result);
        }

        // TODO: PERHAPS DO NOT RAISE AN ERROR????
        // TODO, PERHAPS RAISE AN ERROR HERE
        //    Ok("".to_string())
        Err(format!(
            "VLOOKUP: Search value {lookup_value} does not exist at column {:} for table {}",
            result_column_index, self.name
        )
        .into())
    }

    /// Finds the position of a value in a column or row (MATCH function).
    ///
    /// Searches for a specified value and returns its relative position. This is
    /// equivalent to Excel's MATCH function.
    ///
    /// # Arguments
    ///
    /// * `match_value` - The value to search for
    /// * `match_type` - Determines the match behavior:
    ///   - `Some(1)` or `None` (default): Finds largest value <= match_value (data must be ascending)
    ///   - `Some(0)`: Finds exact match only
    ///   - `Some(-1)`: Finds smallest value >= match_value (data must be descending)
    /// * `column` - The column(s) to search in (e.g., `"c1"` or `"c1,c2,c3"` for horizontal)
    /// * `row` - If `0`, performs vertical search in column; otherwise, performs horizontal
    ///   search in the specified row
    /// * `value_format` - Formatting options including decimal separator
    ///
    /// # Returns
    ///
    /// The 1-based position of the matching value.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The value is not found
    /// - Invalid match_type is provided
    /// - Column identifier is invalid
    async fn match_table(
        &self,
        match_value: &str,
        match_type: Option<i32>,
        column: &str,
        row: u32,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Validate column identifier - may be a comma-separated list
        validate_column_list(column)?;

        let match_type = match_type.unwrap_or(1);

        if row == 0 {
            let (match_value, match_column_value) = self.search_value_column(
                match_value,
                true,
                &value_format.decimal_separator,
                column,
            )?;
            // VERTICAL COLUMN SEARCH
            let sql_query = match match_type {
                -1 => {
                    format!("SELECT c0 FROM {} WHERE {match_column_value} >= {match_value} ORDER BY {column} ASC, c0 ASC LIMIT 1", self.name)
                }
                0 => {
                    format!(
                        "SELECT c0 FROM {} WHERE {match_column_value} = {match_value} LIMIT 1",
                        self.name
                    )
                }
                1 => {
                    format!("SELECT c0 FROM {} WHERE {match_column_value} <= {match_value} ORDER BY {column} DESC, c0 DESC LIMIT 1", self.name)
                }
                _ => {
                    return Err(format!(
                        "MATCH: The match_type must be -1, 0 or 1.  {match_type:} is not permitted"
                    )
                    .into());
                }
            };

            if let Ok(result) = self.sql_query_response(&sql_query).await {
                return Ok(result);
            }
        } else {
            // HORIZONTAL ROW SEARCH
            // row is a u32, so it's safe to use directly in the query
            let match_value = search_value_pure(match_value, true, &value_format.decimal_separator);
            let sql_query = format!(
                "SELECT {column} FROM {} WHERE c0 = {:} LIMIT 1",
                self.name, row
            );
            let value_result = self.sql_query_responses(&sql_query).await?;
            if let Ok(Some(result)) =
                process_horizontal_match_results(&match_value, match_type, None, value_result)
            {
                return Ok(result);
            }
        }

        // TODO: PERHAPS DO NOT RAISE AN ERROR????
        // TODO, PERHAPS RAISE AN ERROR HERE
        //    Ok("".to_string())
        Err(format!(
            "MATCH: Search value {match_value} does not exist for table {} and match type {:}",
            self.name, match_type
        )
        .into())
    }

    /// Retrieves a value at a specific row and column position (INDEX function).
    ///
    /// Returns the value at the intersection of a row and column, or an entire row
    /// or column when one index is 0. This is equivalent to Excel's INDEX function.
    ///
    /// # Arguments
    ///
    /// * `row` - The row number (1-based), or 0 to return an entire column
    /// * `column` - The column number (1-based), or `Some(0)`/`None` to return an entire row.
    ///   When `None` and table has multiple rows, returns the specified row.
    /// * `table_functions` - Optional functions to apply to the result value(s)
    /// * `input` - Input context for function evaluation
    /// * `value_format` - Formatting options
    ///
    /// # Returns
    ///
    /// - Single `Value` when both row and column are specified
    /// - `Value::VecValue` when returning an entire row or column
    /// - `Value::AreaValue` when both row and column are 0 (entire table)
    ///
    /// # Errors
    ///
    /// Returns an error if the specified row/column position does not exist.
    async fn index(
        &self,
        row: i32,
        column: Option<i32>,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let column = if let Some(column) = &column {
            *column
        } else {
            -1
        };

        if row == 0 && column == 0 {
            let sql_query = format!(
                "SELECT {} FROM {}",
                self.generate_column_string(),
                self.name
            );

            let value_result = self.sql_query_area_responses(&sql_query).await;

            if let Some(result) =
                process_area_results(value_result, table_functions, input, value_format).await
            {
                return Ok(result);
            }
        } else if row == 0 {
            let sql_query = if column != -1 {
                format!("SELECT c{:} FROM {}", column, self.name)
            } else {
                format!(
                    "SELECT {} FROM {}",
                    self.generate_column_string(),
                    self.name
                )
            };

            let value_result = self.sql_query_responses(&sql_query).await;

            if let Some(result) =
                process_results(value_result, table_functions, input, value_format).await
            {
                return Ok(result);
            }
        } else if column != -1 {
            if column == 0 {
                let sql_query = format!(
                    "SELECT {} FROM {} WHERE c0 = {:} LIMIT 1",
                    self.generate_column_string(),
                    self.name,
                    row
                );

                let value_result = self.sql_query_responses(&sql_query).await;

                if let Some(result) =
                    process_results(value_result, table_functions, input, value_format).await
                {
                    return Ok(result);
                }
            } else {
                let sql_query = format!(
                    "SELECT c{:} FROM {} WHERE c0 = {:} LIMIT 1",
                    column, self.name, row
                );

                let value_result = self.sql_query_response(&sql_query).await;

                if let Some(result) =
                    process_result(value_result, table_functions, input, value_format).await
                {
                    return Ok(result);
                }
            }
        } else {
            let number_rows = self.count_rows();
            if number_rows > 1 {
                let sql_query = format!(
                    "SELECT {} FROM {} WHERE c0 = {:} LIMIT 1",
                    self.generate_column_string(),
                    self.name,
                    row
                );

                let value_result = self.sql_query_responses(&sql_query).await;

                if let Some(result) =
                    process_results(value_result, table_functions, input, value_format).await
                {
                    return Ok(result);
                }
            } else {
                let sql_query = format!("SELECT c{:} FROM {} LIMIT 1", row, self.name);

                let value_result = self.sql_query_response(&sql_query).await;

                if let Some(result) =
                    process_result(value_result, table_functions, input, value_format).await
                {
                    return Ok(result);
                }
            }
        }

        // TODO: PERHAPS DO NOT RAISE AN ERROR????
        // TODO, PERHAPS RAISE AN ERROR HERE
        //    Ok("".to_string())
        let col = if column != -1 {
            column.to_string()
        } else {
            "none".to_string()
        };
        Err(format!(
            "Index: Row {:} and column {:} position does not exist for table {}",
            row, col, self.name
        )
        .into())
    }

    /// Performs a horizontal lookup (HLOOKUP) on the table.
    ///
    /// Searches for a value in the first row and returns a value from the same column
    /// in a specified row. This is equivalent to Excel's HLOOKUP function.
    ///
    /// # Arguments
    ///
    /// * `lookup_value` - The value to search for in the header row
    /// * `row_index` - The row number to return the value from (1-based)
    /// * `range` - If `Some(true)`, finds closest match; if `Some(false)` or `None`,
    ///   requires exact match
    /// * `table_functions` - Optional functions to apply to the result value
    /// * `input` - Input context for function evaluation
    /// * `column` - The columns to search in the header row
    /// * `value_format` - Formatting options including decimal separator
    ///
    /// # Returns
    ///
    /// The value from the specified row in the matching column.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The lookup value is not found in the header row
    /// - The row index is out of bounds
    /// - The query execution fails
    #[allow(clippy::too_many_arguments)]
    async fn h_lookup(
        &self,
        lookup_value: &str,
        row_index: i32,
        range: Option<bool>,
        table_functions: &TableFunctions,
        input: &Input,
        column: &str,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let range = range.unwrap_or_default();

        let match_type = if range { Some(1) } else { Some(0) };

        if let Ok(value) = self
            .match_table(lookup_value, match_type, column, 1, value_format)
            .await
        {
            if let Ok(found_column) = value.i32(value_format) {
                if let Ok(result) = self
                    .index(
                        row_index,
                        Some(found_column),
                        table_functions,
                        input,
                        value_format,
                    )
                    .await
                {
                    return Ok(result);
                }
            }
        }

        // TODO: PERHAPS DO NOT RAISE AN ERROR????
        // TODO, PERHAPS RAISE AN ERROR HERE
        //    Ok("".to_string())
        Err(format!(
            "HLOOKUP: Search value {lookup_value} does not exist at row {:} for table {}",
            row_index, self.name
        )
        .into())
    }

    /// Performs an advanced lookup (XLOOKUP) with flexible match and search modes.
    ///
    /// A more powerful alternative to VLOOKUP/HLOOKUP that supports multiple match modes,
    /// search directions, and a default value when not found. This is equivalent to
    /// Excel's XLOOKUP function.
    ///
    /// # Arguments
    ///
    /// * `lookup_value` - The value to search for
    /// * `search_column` - The column to search in (e.g., `"c1"`)
    /// * `columns` - The column(s) to return values from (e.g., `"c2"` or `"c2,c3"`)
    /// * `row` - If `0`, performs vertical search; otherwise performs horizontal search
    ///   in the specified row
    /// * `if_not_found` - Optional default value to return if no match is found
    /// * `match_mode` - Determines match behavior:
    ///   - `Some(0)` (default): Exact match
    ///   - `Some(-1)`: Exact match or next smallest
    ///   - `Some(1)`: Exact match or next largest
    ///   - `Some(2)`: Wildcard match
    /// * `search_mode` - Determines search direction:
    ///   - `Some(1)` (default): Search first to last
    ///   - `Some(-1)`: Search last to first
    ///   - `Some(2)`: Binary search ascending
    ///   - `Some(-2)`: Binary search descending
    /// * `table_functions` - Optional functions to apply to the result
    /// * `input` - Input context for function evaluation
    /// * `value_format` - Formatting options including decimal separator
    ///
    /// # Returns
    ///
    /// The value(s) from the result column(s), or the `if_not_found` value if no match.
    ///
    /// # Errors
    ///
    /// Returns an error if no match is found and `if_not_found` is not provided.
    #[allow(clippy::too_many_arguments)]
    async fn x_lookup(
        &self,
        lookup_value: &str,
        search_column: &str,
        columns: &str,
        row: u32,
        if_not_found: Option<String>,
        match_mode: Option<i32>,
        search_mode: Option<i32>,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Validate column identifiers - search_column may be a comma-separated list
        validate_column_list(columns)?;
        validate_column_list(search_column)?;

        if row == 0 {
            // VERTICAL SEARCH
            let sql_query = self.x_search_query(
                lookup_value,
                search_column,
                columns,
                match_mode,
                search_mode,
                value_format,
            )?;

            if !sql_query.is_empty() {
                let single_response = !columns.contains(',');
                if single_response {
                    let value_result = self.sql_query_response(&sql_query).await;
                    if let Some(result) =
                        process_result(value_result, table_functions, input, value_format).await
                    {
                        return Ok(result);
                    }
                } else {
                    let value_result = self.sql_query_responses(&sql_query).await;
                    if let Some(result) =
                        process_results(value_result, table_functions, input, value_format).await
                    {
                        return Ok(result);
                    }
                }
            }
        } else {
            // HORIZONTAL SEARCH
            // row is a u32, so it's safe to use directly in the query
            let match_mode = match_mode.unwrap_or_default();
            let match_value =
                search_value_pure(lookup_value, true, &value_format.decimal_separator);
            let sql_query = format!(
                "SELECT {search_column} FROM {} WHERE c0 = {:} LIMIT 1",
                self.name, row
            );
            let value_result = self.sql_query_responses(&sql_query).await?;
            if let Ok(Some(result)) = process_horizontal_match_results(
                &match_value,
                match_mode,
                search_mode,
                value_result,
            ) {
                // result.i32() returns a validated integer, safe to use in query
                let col_index = result.i32(value_format)?;
                let col_name = format!("c{}", col_index);
                validate_sql_identifier(&col_name)?;
                let sql_query = format!(
                    "SELECT {} FROM {} WHERE c0 <> {:}",
                    col_name, self.name, row
                );
                let value_result = self.sql_query_responses(&sql_query).await;
                if let Some(result) =
                    process_results(value_result, table_functions, input, value_format).await
                {
                    return Ok(result);
                }
            }
        }

        // TODO: PERHAPS DO NOT RAISE AN ERROR????
        // TODO, PERHAPS RAISE AN ERROR HERE
        //    Ok("".to_string())
        if let Some(not_found) = if_not_found {
            Ok(Value::String(not_found))
        } else {
            Err(format!(
                "XSEARCH: Search value {lookup_value} does not exist for table {}",
                self.name
            )
            .into())
        }
    }

    /// Performs a standard lookup operation (LOOKUP function).
    ///
    /// Searches for a value and returns a corresponding value from another column.
    /// This is a simplified wrapper around `x_lookup` with match mode set to find
    /// the largest value less than or equal to the lookup value.
    ///
    /// # Arguments
    ///
    /// * `lookup_value` - The value to search for
    /// * `search_column` - The column to search in
    /// * `columns` - The column(s) to return values from
    /// * `row` - If `0`, performs vertical search; otherwise performs horizontal search
    /// * `table_functions` - Optional functions to apply to the result
    /// * `input` - Input context for function evaluation
    /// * `value_format` - Formatting options
    ///
    /// # Returns
    ///
    /// The value from the result column in the matching row.
    ///
    /// # Errors
    ///
    /// Returns an error if no match is found.
    #[allow(clippy::too_many_arguments)]
    async fn lookup(
        &self,
        lookup_value: &str,
        search_column: &str,
        columns: &str,
        row: u32,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // We are using x_lookup for lookup
        self.x_lookup(
            lookup_value,
            search_column,
            columns,
            row,
            None,
            Some(-1),
            None,
            table_functions,
            input,
            value_format,
        )
        .await
    }

    /// Finds the position of a value with advanced match and search modes (XMATCH function).
    ///
    /// An enhanced version of MATCH that supports additional match modes and search
    /// directions. This is equivalent to Excel's XMATCH function.
    ///
    /// # Arguments
    ///
    /// * `match_value` - The value to search for
    /// * `match_mode` - Determines match behavior:
    ///   - `Some(0)` (default): Exact match
    ///   - `Some(-1)`: Exact match or next smallest
    ///   - `Some(1)`: Exact match or next largest
    ///   - `Some(2)`: Wildcard match
    /// * `search_mode` - Determines search direction:
    ///   - `Some(1)` (default): Search first to last
    ///   - `Some(-1)`: Search last to first
    ///   - `Some(2)`: Binary search ascending
    ///   - `Some(-2)`: Binary search descending
    /// * `column` - The column(s) to search in
    /// * `row` - If `0`, performs vertical search; otherwise performs horizontal search
    ///   in the specified row
    /// * `value_format` - Formatting options including decimal separator
    ///
    /// # Returns
    ///
    /// The 1-based position of the matching value.
    ///
    /// # Errors
    ///
    /// Returns an error if no match is found.
    #[allow(clippy::too_many_arguments)]
    async fn x_match(
        &self,
        match_value: &str,
        match_mode: Option<i32>,
        search_mode: Option<i32>,
        column: &str,
        row: u32,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Validate column identifier - may be a comma-separated list
        validate_column_list(column)?;

        if row == 0 {
            // VERTICAL COLUMN SEARCH
            let sql_query = self.x_search_query(
                match_value,
                column,
                "c0",
                match_mode,
                search_mode,
                value_format,
            )?;

            if !sql_query.is_empty() {
                if let Ok(result) = self.sql_query_response(&sql_query).await {
                    return Ok(result);
                }
            }
        } else {
            // row is a u32, so it's safe to use directly in the query
            let match_mode = match_mode.unwrap_or_default();
            let match_value = search_value_pure(match_value, true, &value_format.decimal_separator);
            let sql_query = format!(
                "SELECT {column} FROM {} WHERE c0 = {:} LIMIT 1",
                self.name, row
            );
            let value_result = self.sql_query_responses(&sql_query).await?;
            if let Ok(Some(result)) = process_horizontal_match_results(
                &match_value,
                match_mode,
                search_mode,
                value_result,
            ) {
                return Ok(result);
            }
        }

        // TODO: PERHAPS DO NOT RAISE AN ERROR????
        // TODO, PERHAPS RAISE AN ERROR HERE
        //    Ok("".to_string())
        Err(format!(
            "XMATCH: Search value {match_value} does not exist for table {}",
            self.name
        )
        .into())
    }

    /// Filters rows based on a condition (FILTER function).
    ///
    /// Returns all rows that match the specified condition. This is equivalent to
    /// Excel's FILTER function.
    ///
    /// # Arguments
    ///
    /// * `condition` - The filter condition to apply (e.g., column comparisons)
    /// * `if_empty` - Value to return if no rows match. If empty string, returns `#CALC!`
    /// * `columns` - The column(s) to return (e.g., `"c1,c2,c3"`)
    /// * `table_functions` - Optional functions to apply to result values
    /// * `input` - Input context for function evaluation
    /// * `value_format` - Formatting options
    ///
    /// # Returns
    ///
    /// A `Value::AreaValue` containing all matching rows, or the `if_empty` value
    /// if no rows match.
    ///
    /// # Errors
    ///
    /// Returns an error if column identifiers are invalid.
    #[allow(clippy::too_many_arguments)]
    async fn filter(
        &self,
        condition: Condition,
        if_empty: &str,
        columns: &str,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Validate column identifiers
        validate_column_list(columns)?;

        let abstract_column_types = self.get_abstract_column_types();
        // Note: condition.condition() should also perform validation/escaping internally
        let where_condition = condition.condition(abstract_column_types, value_format)?;

        let sql_query = format!(
            "SELECT {columns} FROM {} WHERE {where_condition}",
            self.name
        );

        let value_result = self.sql_query_area_responses(&sql_query).await;
        if let Some(result) =
            process_area_results(value_result, table_functions, input, value_format).await
        {
            Ok(result)
        } else if if_empty.is_empty() {
            // TODO: CHECK IF WE SHOULD RAISE AN ERROR HERE INSTEAD???
            Ok(Value::String("#CALC!".to_string()))
        } else {
            Ok(Value::String(if_empty.to_string()))
        }
    }

    /// Retrieves all rows from the table for specified columns.
    ///
    /// Returns the complete contents of the specified columns as a 2D array.
    ///
    /// # Arguments
    ///
    /// * `columns` - The column(s) to return (e.g., `"c1,c2,c3"`)
    /// * `table_functions` - Optional functions to apply to result values
    /// * `input` - Input context for function evaluation
    /// * `value_format` - Formatting options
    ///
    /// # Returns
    ///
    /// A `Value::AreaValue` containing all rows for the specified columns.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Column identifiers are invalid
    /// - The table is empty
    async fn select_all(
        &self,
        columns: &str,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Validate column identifiers
        validate_column_list(columns)?;

        let sql_query = format!("SELECT {columns} FROM {}", self.name);

        let value_result = self.sql_query_area_responses(&sql_query).await;
        if let Some(result) =
            process_area_results(value_result, table_functions, input, value_format).await
        {
            Ok(result)
        } else {
            Err("ALL: No values found".into())
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn filter_with_modifiers(
        &self,
        condition: Condition,
        if_empty: &str,
        columns: &str,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
        modifiers: &SqlModifiers,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if modifiers.is_empty() {
            return self
                .filter(
                    condition,
                    if_empty,
                    columns,
                    table_functions,
                    input,
                    value_format,
                )
                .await;
        }

        validate_column_list(columns)?;

        let abstract_column_types = self.get_abstract_column_types();
        let where_condition = condition.condition(abstract_column_types, value_format)?;

        let col_list: Vec<String> = columns
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let (select_prefix, order_clause, limit_clause) =
            build_parquet_modifier_clauses(modifiers, &col_list);

        // Aggregate query: returns a single scalar value
        if let Some(ref agg) = modifiers.aggregate {
            let select_expr = if matches!(agg, SqlAggregate::Count | SqlAggregate::CountA) {
                "COUNT(*)".to_string()
            } else {
                // Filter to numeric columns only, matching Excel behavior of ignoring text
                let numeric_cols: Vec<String> = col_list
                    .iter()
                    .filter(|col| {
                        abstract_column_types
                            .get(*col)
                            .is_some_and(|ct| ct.is_numeric())
                    })
                    .cloned()
                    .collect();
                if numeric_cols.is_empty() {
                    // Fallback: use first column (preserves existing behavior)
                    format!("{}({})", agg.sql_function(), col_list[0])
                } else {
                    agg.build_aggregate_select(&numeric_cols)
                }
            };
            let sql_query = format!(
                "SELECT {} FROM {} WHERE {}",
                select_expr, self.name, where_condition
            );
            let batches = self
                .sql_query_name_area_responses(&self.name, &self.filename, &sql_query)
                .await?;
            // Aggregate returns a single row with single column
            if let Some(first_row) = batches.first() {
                if let Some(val) = first_row.first() {
                    return Ok(val.clone());
                }
            }
            return Ok(Value::F64(0.0));
        }

        // Non-aggregate query with modifiers
        let sql_query = format!(
            "SELECT {select_prefix}{columns} FROM {} WHERE {where_condition}{order_clause}{limit_clause}",
            self.name
        );

        let value_result = self.sql_query_area_responses(&sql_query).await;
        if let Some(result) =
            process_area_results(value_result, table_functions, input, value_format).await
        {
            Ok(result)
        } else if if_empty.is_empty() {
            Ok(Value::String("#CALC!".to_string()))
        } else {
            Ok(Value::String(if_empty.to_string()))
        }
    }

    async fn select_all_with_modifiers(
        &self,
        columns: &str,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
        modifiers: &SqlModifiers,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if modifiers.is_empty() {
            return self
                .select_all(columns, table_functions, input, value_format)
                .await;
        }

        validate_column_list(columns)?;

        let col_list: Vec<String> = columns
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let (select_prefix, order_clause, limit_clause) =
            build_parquet_modifier_clauses(modifiers, &col_list);

        // Aggregate query
        if let Some(ref agg) = modifiers.aggregate {
            let select_expr = if matches!(agg, SqlAggregate::Count | SqlAggregate::CountA) {
                "COUNT(*)".to_string()
            } else {
                let abstract_column_types = self.get_abstract_column_types();
                let numeric_cols: Vec<String> = col_list
                    .iter()
                    .filter(|col| {
                        abstract_column_types
                            .get(*col)
                            .is_some_and(|ct| ct.is_numeric())
                    })
                    .cloned()
                    .collect();
                if numeric_cols.is_empty() {
                    format!("{}({})", agg.sql_function(), col_list[0])
                } else {
                    agg.build_aggregate_select(&numeric_cols)
                }
            };
            let sql_query = format!("SELECT {} FROM {}", select_expr, self.name);
            let batches = self
                .sql_query_name_area_responses(&self.name, &self.filename, &sql_query)
                .await?;
            if let Some(first_row) = batches.first() {
                if let Some(val) = first_row.first() {
                    return Ok(val.clone());
                }
            }
            return Ok(Value::F64(0.0));
        }

        let sql_query = format!(
            "SELECT {select_prefix}{columns} FROM {}{order_clause}{limit_clause}",
            self.name
        );

        let value_result = self.sql_query_area_responses(&sql_query).await;
        if let Some(result) =
            process_area_results(value_result, table_functions, input, value_format).await
        {
            Ok(result)
        } else {
            Err("ALL: No values found".into())
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn x_lookup_with_modifiers(
        &self,
        lookup_value: &str,
        search_column: &str,
        columns: &str,
        row: u32,
        if_not_found: Option<String>,
        match_mode: Option<i32>,
        search_mode: Option<i32>,
        table_functions: &TableFunctions,
        input: &Input,
        value_format: &ValueFormat,
        modifiers: &SqlModifiers,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        if modifiers.is_empty() {
            return self
                .x_lookup(
                    lookup_value,
                    search_column,
                    columns,
                    row,
                    if_not_found,
                    match_mode,
                    search_mode,
                    table_functions,
                    input,
                    value_format,
                )
                .await;
        }

        // For x_lookup with modifiers, delegate to base for now
        self.x_lookup(
            lookup_value,
            search_column,
            columns,
            row,
            if_not_found,
            match_mode,
            search_mode,
            table_functions,
            input,
            value_format,
        )
        .await
    }

    /// Adds a new row to the table.
    ///
    /// **Not implemented.** `ParquetTable` is read-only and does not support write operations.
    ///
    /// # Errors
    ///
    /// Always returns an error indicating the operation is not supported.
    async fn add_row(
        &self,
        _values: Vec<Value>,
        _table_functions: &TableFunctions,
        _input: &Input,
        _value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        Err("ADDROW: Not implemented for read only tables".into())
    }

    /// Updates an existing row in the table.
    ///
    /// **Not implemented.** `ParquetTable` is read-only and does not support write operations.
    ///
    /// # Errors
    ///
    /// Always returns an error indicating the operation is not supported.
    async fn update_row(
        &self,
        _id: &str,
        _values: Vec<Value>,
        _table_functions: &TableFunctions,
        _input: &Input,
        _value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        Err("UPDATEROW: Not implemented for read only tables".into())
    }

    /// Deletes a row from the table.
    ///
    /// **Not implemented.** `ParquetTable` is read-only and does not support write operations.
    ///
    /// # Errors
    ///
    /// Always returns an error indicating the operation is not supported.
    async fn delete_row(
        &self,
        _id: &str,
        _table_functions: &TableFunctions,
        _input: &Input,
        _value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        Err("DELETEROW: Not implemented for read only tables".into())
    }

    /// Reads a specific row by ID.
    ///
    /// **Not implemented.** Use `index()` or `filter()` instead for row retrieval.
    ///
    /// # Errors
    ///
    /// Always returns an error indicating the operation is not supported.
    async fn read_row(
        &self,
        _id: &str,
        _table_functions: &TableFunctions,
        _input: &Input,
        _value_format: &ValueFormat,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        Err("READROW: Not implemented for read only tables".into())
    }
}
