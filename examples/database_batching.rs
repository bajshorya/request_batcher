/// Example: Batching database queries to reduce round trips
///
/// This example shows how to batch multiple individual database queries
/// into efficient bulk operations.

use async_trait::async_trait;
use request_batcher::{BatchProcessor, Batcher};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Table {
    Users,
    Orders,
    Products,
}

type RecordId = i64;

#[derive(Debug, Clone)]
struct Record {
    id: i64,
    data: String,
}

#[derive(Debug, Clone)]
struct DbError(String);

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Database Error: {}", self.0)
    }
}

impl std::error::Error for DbError {}

/// Simulated database client
struct DatabaseClient;

#[async_trait]
impl BatchProcessor<Table, RecordId, Record, DbError> for DatabaseClient {
    async fn process_batch(
        &self,
        table: Table,
        ids: Vec<RecordId>,
    ) -> Result<HashMap<RecordId, Record>, DbError> {
        println!(
            "🗄️  Executing batch query on {:?} for {} IDs: {:?}",
            table, ids.len(), ids
        );

        // Simulate database query delay
        sleep(Duration::from_millis(30)).await;

        // Simulate bulk SELECT query: SELECT * FROM table WHERE id IN (...)
        let mut results = HashMap::new();
        for id in ids {
            results.insert(
                id,
                Record {
                    id,
                    data: format!("Data from {:?} table for ID {}", table, id),
                },
            );
        }

        println!("✅ Query completed, fetched {} records", results.len());
        Ok(results)
    }
}

#[tokio::main]
async fn main() {
    println!("=== Database Query Batching Example ===\n");

    let processor = Arc::new(DatabaseClient);
    let batcher = Batcher::new(
        processor,
        50,  // 50ms batch window
        100, // max 100 queries per batch
    );

    println!("Scenario: Multiple services requesting user data simultaneously\n");

    // Simulate multiple services making concurrent database queries
    let mut handles = vec![];
    
    // Service A requests users 1-3
    for id in 1..=3 {
        let batcher_clone = batcher.clone();
        let handle = tokio::spawn(async move {
            println!("🔵 Service A requesting user {}", id);
            match batcher_clone.submit(Table::Users, id).await {
                Ok(record) => println!("  ✓ Service A got: {}", record.data),
                Err(e) => eprintln!("  ✗ Service A error: {:?}", e),
            }
        });
        handles.push(handle);
    }

    // Service B requests users 3-5 (note: user 3 is duplicate)
    for id in 3..=5 {
        let batcher_clone = batcher.clone();
        let handle = tokio::spawn(async move {
            println!("🟢 Service B requesting user {}", id);
            match batcher_clone.submit(Table::Users, id).await {
                Ok(record) => println!("  ✓ Service B got: {}", record.data),
                Err(e) => eprintln!("  ✗ Service B error: {:?}", e),
            }
        });
        handles.push(handle);
    }

    // Wait for all queries
    for handle in handles {
        handle.await.unwrap();
    }

    println!("\n=== Results ===");
    println!("✨ All 6 individual queries were batched into 1 database call!");
    println!("✨ Duplicate request (user 3) was automatically deduplicated!");

    // Now demonstrate size-based batching
    println!("\n=== Testing Size-Based Batching ===\n");
    
    let batcher_small = Batcher::new(
        Arc::new(DatabaseClient),
        5000, // Very long window (5 seconds)
        3,    // But small batch size (3 requests)
    );

    println!("Submitting 5 requests with max_batch_size=3...\n");
    
    let mut handles = vec![];
    for id in 10..=14 {
        let batcher_clone = batcher_small.clone();
        let handle = tokio::spawn(async move {
            println!("📤 Submitting request for ID {}", id);
            let _ = batcher_clone.submit(Table::Orders, id).await;
        });
        handles.push(handle);
        
        // Small delay to show batching behavior
        sleep(Duration::from_millis(10)).await;
    }

    for handle in handles {
        handle.await.unwrap();
    }

    println!("\n✨ First 3 requests triggered immediate batch (size limit)");
    println!("✨ Remaining 2 requests will batch after timeout");
    
    // Wait for the timeout batch
    sleep(Duration::from_millis(100)).await;
}
