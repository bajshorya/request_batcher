use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

/// Trait for implementing custom batch processing logic
///
/// # Type Parameters
///
/// * `R` - Resource type (e.g., API endpoint, database table, chain type)
/// * `Req` - Individual request type (e.g., user ID, query parameters)
/// * `Res` - Individual response type (e.g., user data, query results)
/// * `E` - Error type for batch processing failures
///
/// # Example
///
/// ```rust
/// use request_batcher::BatchProcessor;
/// use std::collections::HashMap;
/// use async_trait::async_trait;
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// enum Database {
///     Users,
///     Posts,
/// }
///
/// #[derive(Debug, Clone)]
/// struct DbError(String);
///
/// impl std::fmt::Display for DbError {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "{}", self.0)
///     }
/// }
///
/// impl std::error::Error for DbError {}
///
/// struct MyProcessor;
///
/// #[async_trait]
/// impl BatchProcessor<Database, i64, String, DbError> for MyProcessor {
///     async fn process_batch(
///         &self,
///         resource: Database,
///         requests: Vec<i64>,
///     ) -> Result<HashMap<i64, String>, DbError> {
///         // Fetch data for all IDs in one query
///         let mut results = HashMap::new();
///         for id in requests {
///             results.insert(id, format!("Data for {}", id));
///         }
///         Ok(results)
///     }
/// }
/// ```
#[async_trait]
pub trait BatchProcessor<R, Req, Res, E>: Send + Sync
where
    R: Send + Sync + Clone + Eq + Hash + Debug,
    Req: Send + Sync + Clone + Eq + Hash + Debug,
    Res: Send + Sync + Clone + Debug,
    E: std::error::Error + Send + Sync + Clone,
{
    /// Process a batch of requests for a specific resource
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource type being queried
    /// * `requests` - Vector of individual requests to process as a batch
    ///
    /// # Returns
    ///
    /// A HashMap mapping each request to its response. All requests in the input
    /// should have corresponding entries in the output HashMap.
    async fn process_batch(
        &self,
        resource: R,
        requests: Vec<Req>,
    ) -> Result<HashMap<Req, Res>, E>;
}
