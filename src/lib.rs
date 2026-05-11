//! # Request Batcher
//!
//! A generic async request batching library for Rust that combines multiple individual
//! requests into batches based on time windows and size limits.
//!
//! ## Features
//!
//! - **Time-window batching**: Automatically batch requests within a configurable time window
//! - **Size-based triggers**: Process batches immediately when they reach a maximum size
//! - **Resource-type isolation**: Separate queues for different resource types
//! - **Async response distribution**: Each request gets its individual response
//! - **Generic and flexible**: Works with any request/response types
//!
//! ## Example
//!
//! ```rust
//! use request_batcher::{Batcher, BatchProcessor};
//! use std::sync::Arc;
//! use async_trait::async_trait;
//!
//! // Define your resource types
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
//! enum ApiEndpoint {
//!     Users,
//!     Posts,
//! }
//!
//! // Define your error type
//! #[derive(Debug, Clone)]
//! struct ApiError(String);
//!
//! impl std::fmt::Display for ApiError {
//!     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//!         write!(f, "{}", self.0)
//!     }
//! }
//!
//! impl std::error::Error for ApiError {}
//!
//! // Define your batch processor
//! struct MyBatchProcessor;
//!
//! #[async_trait]
//! impl BatchProcessor<ApiEndpoint, String, Vec<String>, ApiError> for MyBatchProcessor {
//!     async fn process_batch(
//!         &self,
//!         resource: ApiEndpoint,
//!         requests: Vec<String>,
//!     ) -> Result<std::collections::HashMap<String, Vec<String>>, ApiError> {
//!         // Your batch processing logic here
//!         Ok(std::collections::HashMap::new())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let processor = Arc::new(MyBatchProcessor);
//!     let batcher = Batcher::new(processor, 100, 50); // 100ms window, max 50 requests
//!     
//!     // Submit requests
//!     let result = batcher.submit(ApiEndpoint::Users, "user123".to_string()).await;
//! }
//! ```

mod batcher;
mod error;
mod processor;

pub use batcher::{Batcher, BatcherConfig};
pub use error::BatchError;
pub use processor::BatchProcessor;
