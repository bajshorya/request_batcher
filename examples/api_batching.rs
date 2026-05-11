/// Example: Batching API requests to reduce HTTP calls
///
/// This example demonstrates how to use the request batcher to combine
/// multiple individual API requests into efficient batch calls.

use async_trait::async_trait;
use request_batcher::{BatchProcessor, Batcher};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ApiEndpoint {
    Users,
    Posts,
    Comments,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UserId(String);

#[derive(Debug, Clone)]
struct UserData {
    id: String,
    name: String,
    email: String,
}

#[derive(Debug, Clone)]
struct ApiError(String);

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "API Error: {}", self.0)
    }
}

impl std::error::Error for ApiError {}

/// Simulated API client that batches requests
struct ApiClient;

#[async_trait]
impl BatchProcessor<ApiEndpoint, UserId, UserData, ApiError> for ApiClient {
    async fn process_batch(
        &self,
        endpoint: ApiEndpoint,
        user_ids: Vec<UserId>,
    ) -> Result<HashMap<UserId, UserData>, ApiError> {
        println!(
            "🚀 Processing batch for {:?} with {} requests",
            endpoint,
            user_ids.len()
        );

        // Simulate API call delay
        sleep(Duration::from_millis(50)).await;

        // Simulate fetching data for all users in one API call
        let mut results = HashMap::new();
        for user_id in user_ids {
            results.insert(
                user_id.clone(),
                UserData {
                    id: user_id.0.clone(),
                    name: format!("User {}", user_id.0),
                    email: format!("user{}@example.com", user_id.0),
                },
            );
        }

        println!("✅ Batch completed with {} results", results.len());
        Ok(results)
    }
}

#[tokio::main]
async fn main() {
    println!("=== API Request Batching Example ===\n");

    let processor = Arc::new(ApiClient);
    let batcher = Batcher::new(
        processor,
        100, // 100ms batch window
        10,  // max 10 requests per batch
    );

    println!("Submitting 5 individual requests rapidly...\n");

    // Spawn multiple concurrent requests
    let mut handles = vec![];
    for i in 1..=5 {
        let batcher_clone = batcher.clone();
        let handle = tokio::spawn(async move {
            let user_id = UserId(format!("{}", i));
            println!("📤 Submitting request for user {}", i);
            
            match batcher_clone.submit(ApiEndpoint::Users, user_id).await {
                Ok(user_data) => {
                    println!("📥 Received: {} ({})", user_data.name, user_data.email);
                }
                Err(e) => {
                    eprintln!("❌ Error: {:?}", e);
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        handle.await.unwrap();
    }

    println!("\n=== All requests completed! ===");
    println!("Notice: All 5 requests were batched into a single API call!");

    // Show stats
    let stats = batcher.stats().await;
    println!("\nBatcher Stats:");
    println!("  Total queues: {}", stats.total_queues);
    println!("  Pending requests: {}", stats.total_pending_requests);
    println!("  Processing queues: {}", stats.processing_queues);
}
