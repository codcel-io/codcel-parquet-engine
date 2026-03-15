// SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
// SPDX-License-Identifier: MIT OR Apache-2.0 OR Codcel-Commercial
//
// This file is part of Codcel (https://codcel.io).
// See LICENSE-MIT, LICENSE-APACHE, and LICENSE-CODCEL-COMMERCIAL in the project root.

use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use log::error;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex, broadcast};
use tokio::time;

/// Result type for query execution that can be shared across waiters
type QueryResult = Result<Arc<Vec<RecordBatch>>, QueryError>;

/// Error type for SQL query execution failures.
///
/// This enum represents the various ways a cached SQL query can fail. It implements
/// `Clone` to allow broadcasting errors to multiple waiting requests during request
/// coalescing.
#[derive(Clone, Debug)]
pub enum QueryError {
    /// Query execution failed during DataFusion processing.
    ///
    /// Contains the error message from DataFusion describing what went wrong
    /// during query execution (e.g., type mismatches, missing columns).
    ExecutionError(String),

    /// SQL statement could not be parsed.
    ///
    /// Contains the parse error message. This typically indicates a malformed
    /// SQL query string.
    ParseError(String),

    /// Failed to register the Parquet file with DataFusion.
    ///
    /// Contains the registration error message. This can occur if the file
    /// doesn't exist, is not a valid Parquet file, or cannot be read.
    RegistrationError(String),

    /// The in-flight query was abandoned before completing.
    ///
    /// This occurs when the leader task executing the query is cancelled or
    /// dropped before it can broadcast results to waiting followers.
    Abandoned,

    /// Timed out waiting for an in-flight query to complete.
    ///
    /// This occurs when a follower request waits longer than the configured
    /// `in_flight_timeout` for the leader to finish executing the query.
    Timeout,
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::ExecutionError(e) => write!(f, "Query execution failed: {}", e),
            QueryError::ParseError(e) => write!(f, "SQL parsing failed: {}", e),
            QueryError::RegistrationError(e) => write!(f, "Table registration failed: {}", e),
            QueryError::Abandoned => write!(f, "In-flight query was abandoned"),
            QueryError::Timeout => write!(f, "Timeout waiting for query result"),
        }
    }
}

impl std::error::Error for QueryError {}

struct CacheEntry {
    result: Arc<Vec<RecordBatch>>,
    cached_at: Instant,
}

/// Tracks an in-flight query execution for request coalescing
struct InFlightQuery {
    /// Sender to broadcast results to all waiters
    sender: broadcast::Sender<QueryResult>,
    /// Timestamp when the query started (for timeout detection)
    started_at: Instant,
}

/// Query execution role for the current request
enum QueryRole {
    /// This request will execute the query (leader)
    Leader,
    /// This request will wait for another request's result (follower)
    Follower(broadcast::Receiver<QueryResult>),
    /// Result was found in cache
    CacheHit(Arc<Vec<RecordBatch>>),
}

/// SQL query cache with request coalescing for Parquet file queries.
///
/// `SqlCache` provides intelligent caching of DataFusion SQL query results with
/// automatic expiration and request coalescing. When multiple identical queries
/// arrive concurrently, only one executes while others wait for the shared result.
///
/// # Features
///
/// - **Result caching**: Query results are cached with a configurable TTL
/// - **Request coalescing**: Concurrent identical queries share a single execution
/// - **Leader/follower pattern**: First request executes (leader), others wait (followers)
/// - **Automatic cleanup**: Background task removes expired entries
/// - **Lazy table registration**: Parquet files are registered on first query
///
/// # Thread Safety
///
/// This struct uses interior mutability via `RwLock` and `Mutex` to allow
/// concurrent query execution with a shared `&self` reference.
pub struct SqlCache {
    cached_results: Arc<RwLock<HashMap<String, CacheEntry>>>,
    /// Track queries currently being executed for request coalescing
    in_flight: Arc<RwLock<HashMap<String, InFlightQuery>>>,
    ttl: Duration,
    /// Timeout for waiting on in-flight queries
    in_flight_timeout: Duration,
    /// Mutex for SessionContext since it requires exclusive access for queries
    ctx: Mutex<SessionContext>,
    /// RwLock for registered_tables to allow concurrent reads
    registered_tables: RwLock<HashSet<String>>,
}

impl SqlCache {
    /// Creates a new SQL cache with the specified TTL.
    ///
    /// Uses a default in-flight timeout of 60 seconds for request coalescing.
    ///
    /// # Arguments
    ///
    /// * `ttl_seconds` - How long query results remain cached before expiration
    ///
    /// # Returns
    ///
    /// A new `SqlCache` instance ready for use.
    pub fn new(ttl_seconds: u64) -> Self {
        Self::with_timeout(ttl_seconds, 60) // Default 60 second in-flight timeout
    }

    /// Creates a new SQL cache with custom TTL and in-flight timeout.
    ///
    /// # Arguments
    ///
    /// * `ttl_seconds` - How long query results remain cached before expiration
    /// * `in_flight_timeout_seconds` - Maximum time a follower request will wait
    ///   for a leader to complete query execution before timing out
    ///
    /// # Returns
    ///
    /// A new `SqlCache` instance with the specified timeout configuration.
    pub fn with_timeout(ttl_seconds: u64, in_flight_timeout_seconds: u64) -> Self {
        Self {
            cached_results: Arc::new(RwLock::new(HashMap::new())),
            in_flight: Arc::new(RwLock::new(HashMap::new())),
            ttl: Duration::new(ttl_seconds, 0),
            in_flight_timeout: Duration::new(in_flight_timeout_seconds, 0),
            ctx: Mutex::new(SessionContext::new()),
            registered_tables: RwLock::new(HashSet::new()),
        }
    }

    /// Executes a SQL query with caching and request coalescing.
    ///
    /// This method checks the cache first, and if not found, executes the query
    /// against the Parquet file using DataFusion. Concurrent identical queries
    /// are coalesced so only one actually executes while others wait for the result.
    ///
    /// The Parquet file is lazily registered with DataFusion on the first query
    /// that references it.
    ///
    /// # Arguments
    ///
    /// * `name` - The table name to register the Parquet file under (used in SQL FROM clause)
    /// * `filename` - Path pattern to the Parquet file(s), supports glob patterns like `*`
    /// * `sql_query` - The SQL query string to execute
    ///
    /// # Returns
    ///
    /// * `Ok(Some(batches))` - Query executed successfully, returns Arrow record batches
    /// * `Ok(None)` - Query could not be parsed (SQL syntax error)
    /// * `Err(e)` - Query failed due to execution error, registration error, timeout, or abandonment
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Parquet file cannot be registered (file not found, invalid format)
    /// - Query execution fails (invalid column names, type errors)
    /// - Timeout occurs while waiting for an in-flight query
    /// - An in-flight query is abandoned by its leader
    pub async fn sql_query(
        &self,
        name: &str,
        filename: &str,
        sql_query: &str,
    ) -> Result<Option<Arc<Vec<RecordBatch>>>, Box<dyn std::error::Error + Send + Sync>> {
        // Phase 1: Determine role (cache hit, leader, or follower)
        let role = self.determine_query_role(sql_query).await;

        match role {
            QueryRole::CacheHit(result) => {
                return Ok(Some(result));
            }
            QueryRole::Follower(receiver) => {
                return self.wait_for_leader(receiver, sql_query).await;
            }
            QueryRole::Leader => {
                // Continue to execute the query
            }
        }

        // Phase 2: Execute query as leader
        let result = self.execute_query_as_leader(name, filename, sql_query).await;

        // Phase 3: Broadcast result and cleanup
        self.complete_in_flight(sql_query, &result).await;

        // Convert internal result to external result type
        match result {
            Ok(batches) => Ok(Some(batches)),
            Err(QueryError::ParseError(_)) => Ok(None), // Match original behavior
            Err(e) => Err(e.into()),
        }
    }

    /// Determine if this request should be a leader, follower, or cache hit.
    /// Uses atomic locking to prevent race conditions between cache check and in-flight registration.
    async fn determine_query_role(&self, sql_query: &str) -> QueryRole {
        // Acquire in_flight write lock first - this ensures atomic check-then-register semantics
        // and prevents the race condition where a request could miss both the cache and in-flight
        // check, leading to duplicate query executions.
        let mut in_flight = self.in_flight.write().await;

        // Step 1: Check cache while holding in_flight lock
        // This prevents: Request A finishes -> removes from in_flight -> Request B checks cache (miss)
        // -> checks in_flight (empty) -> becomes duplicate leader
        {
            let cache = self.cached_results.read().await;
            if let Some(entry) = cache.get(sql_query) {
                return QueryRole::CacheHit(Arc::clone(&entry.result));
            }
        }

        // Step 2: Check if another request is already executing this query
        if let Some(flight) = in_flight.get(sql_query) {
            // Subscribe to the existing broadcast
            let receiver = flight.sender.subscribe();
            return QueryRole::Follower(receiver);
        }

        // Step 3: No one is executing - become the leader
        // Create broadcast channel with capacity 16 to handle late subscribers
        let (sender, _) = broadcast::channel(16);

        in_flight.insert(sql_query.to_string(), InFlightQuery {
            sender,
            started_at: Instant::now(),
        });

        QueryRole::Leader
    }

    /// Wait for the leader to complete the query
    async fn wait_for_leader(
        &self,
        mut receiver: broadcast::Receiver<QueryResult>,
        sql_query: &str,
    ) -> Result<Option<Arc<Vec<RecordBatch>>>, Box<dyn std::error::Error + Send + Sync>> {
        // Wait with timeout
        let result = tokio::time::timeout(
            self.in_flight_timeout,
            receiver.recv()
        ).await;

        match result {
            Ok(Ok(query_result)) => {
                // Successfully received result from leader
                match query_result {
                    Ok(batches) => Ok(Some(batches)),
                    Err(QueryError::ParseError(_)) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                // Leader dropped sender without sending - query abandoned
                self.cleanup_stale_in_flight(sql_query).await;
                Err(QueryError::Abandoned.into())
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                // Missed the broadcast, check cache as fallback
                let cache = self.cached_results.read().await;
                if let Some(entry) = cache.get(sql_query) {
                    Ok(Some(Arc::clone(&entry.result)))
                } else {
                    Err(QueryError::Abandoned.into())
                }
            }
            Err(_timeout) => {
                // Timeout waiting for leader
                error!("Timeout waiting for in-flight query: {}", sql_query);
                Err(QueryError::Timeout.into())
            }
        }
    }

    /// Execute the query (called only by leader)
    async fn execute_query_as_leader(
        &self,
        name: &str,
        filename: &str,
        sql_query: &str,
    ) -> QueryResult {
        // Register the Parquet file only if not already registered
        let needs_registration = {
            let tables = self.registered_tables.read().await;
            !tables.contains(name)
        };

        // Clone the context for query execution - this allows concurrent queries
        // DataFusion's SessionContext is designed to be cloned; clones share the same
        // catalog/schema state but can execute queries independently
        let ctx = {
            let ctx_guard = self.ctx.lock().await;

            if needs_registration {
                // Double-check with write lock to avoid race condition
                let mut tables = self.registered_tables.write().await;
                if !tables.contains(name) {
                    if let Err(e) = ctx_guard.register_parquet(name, filename, ParquetReadOptions::default()).await {
                        return Err(QueryError::RegistrationError(e.to_string()));
                    }
                    tables.insert(name.to_string());
                }
            }

            // Clone the context and immediately release the lock
            ctx_guard.clone()
        };

        // Run the query on the cloned context - all locks released
        match ctx.sql(sql_query).await {
            Ok(dataframe) => {
                match dataframe.collect().await {
                    Ok(result) => {
                        let result = Arc::new(result);

                        // Cache the result
                        {
                            let mut cache = self.cached_results.write().await;
                            cache.insert(
                                sql_query.to_string(),
                                CacheEntry {
                                    result: Arc::clone(&result),
                                    cached_at: Instant::now(),
                                },
                            );
                        }

                        Ok(result)
                    }
                    Err(e) => {
                        error!("Query execution failed: {}", e);
                        Err(QueryError::ExecutionError(e.to_string()))
                    }
                }
            }
            Err(e) => {
                error!("SQL parsing failed: {:?}", e);
                Err(QueryError::ParseError(e.to_string()))
            }
        }
    }

    /// Broadcast result to waiters and clean up in-flight entry
    async fn complete_in_flight(&self, sql_query: &str, result: &QueryResult) {
        let mut in_flight = self.in_flight.write().await;

        if let Some(flight) = in_flight.remove(sql_query) {
            // Broadcast result to all waiters
            // Ignore send errors - receivers may have already timed out or been dropped
            let _ = flight.sender.send(result.clone());
        }
    }

    /// Clean up potentially stale in-flight entry (called by followers on error)
    async fn cleanup_stale_in_flight(&self, sql_query: &str) {
        let mut in_flight = self.in_flight.write().await;

        // Only remove if it's been there longer than expected (stale)
        if let Some(flight) = in_flight.get(sql_query) {
            if flight.started_at.elapsed() > self.in_flight_timeout {
                in_flight.remove(sql_query);
            }
        }
    }

    /// Starts a background task that periodically cleans up expired cache entries.
    ///
    /// This spawns a Tokio task that runs indefinitely, removing:
    /// - Cached query results older than the configured TTL
    /// - Stale in-flight query entries (queries that took too long)
    ///
    /// The cleanup runs at half the TTL interval for balanced efficiency.
    /// This method should be called once after creating the cache.
    ///
    /// # Panics
    ///
    /// This method must be called from within a Tokio runtime context.
    pub fn start_cleanup_task(&self) {
        let cache = Arc::clone(&self.cached_results);
        let in_flight = Arc::clone(&self.in_flight);
        let ttl = self.ttl;
        let in_flight_timeout = self.in_flight_timeout;

        tokio::spawn(async move {
            // Run cleanup twice per TTL period for balanced efficiency
            let cleanup_interval = ttl / 2;
            let mut interval = time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                let now = Instant::now();

                // Clean up expired cache entries
                {
                    let mut cache_guard = cache.write().await;
                    cache_guard.retain(|_, entry| now.duration_since(entry.cached_at) < ttl);
                }

                // Clean up stale in-flight entries (queries that took too long)
                {
                    let mut in_flight_guard = in_flight.write().await;
                    in_flight_guard.retain(|_, flight| {
                        flight.started_at.elapsed() < in_flight_timeout * 3
                    });
                }
            }
        });
    }
}
