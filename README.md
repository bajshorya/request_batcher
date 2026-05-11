# 🚀 Request Batcher

A high-performance, generic async request batching library for Rust that intelligently combines multiple individual requests into efficient batches.

[![Crates.io](https://img.shields.io/crates/v/request-batcher.svg)](https://crates.io/crates/request-batcher)
[![Documentation](https://docs.rs/request-batcher/badge.svg)](https://docs.rs/request-batcher)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

## ✨ Features

- **⏱️ Time-Window Batching**: Automatically batch requests within a configurable time window
- **📦 Size-Based Triggers**: Process batches immediately when they reach a maximum size
- **🎯 Resource-Type Isolation**: Separate queues for different resource types (APIs, databases, etc.)
- **🔄 Async Response Distribution**: Each request gets its individual response
- **🧬 Generic & Flexible**: Works with any request/response types
- **🔍 Request Deduplication**: Automatically deduplicates identical requests in a batch
- **📊 Built-in Statistics**: Monitor queue sizes and processing state
- **🦀 Zero-Copy**: Efficient memory usage with Arc and smart cloning

## 🎯 Use Cases

- **API Rate Limiting**: Combine multiple API calls into batch requests
- **Database Query Optimization**: Batch individual queries into bulk operations
- **Blockchain RPC Calls**: Reduce network overhead with multicall patterns
- **Microservice Communication**: Optimize inter-service requests
- **Cache Warming**: Efficiently batch cache miss requests
- **GraphQL DataLoader Pattern**: Implement efficient data loading

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
request-batcher = "0.1"
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
```

## 🚀 Quick Start

```rust
use request_batcher::{Batcher, BatchProcessor};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

// 1. Define your resource types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ApiEndpoint {
    Users,
    Posts,
}

// 2. Define your error type
#[derive(Debug, Clone)]
struct ApiError(String);

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ApiError {}

// 3. Implement the BatchProcessor trait
struct MyApiClient;

#[async_trait]
impl BatchProcessor<ApiEndpoint, String, String, ApiError> for MyApiClient {
    async fn process_batch(
        &self,
        endpoint: ApiEndpoint,
        user_ids: Vec<String>,
    ) -> Result<HashMap<String, String>, ApiError> {
        // Your batch processing logic here
        // e.g., make a single API call for all user_ids
        let mut results = HashMap::new();
        for id in user_ids {
            results.insert(id.clone(), format!("Data for {}", id));
        }
        Ok(results)
    }
}

#[tokio::main]
async fn main() {
    // 4. Create the batcher
    let processor = Arc::new(MyApiClient);
    let batcher = Batcher::new(
        processor,
        100, // 100ms batch window
        50,  // max 50 requests per batch
    );

    // 5. Submit requests
    let result = batcher
        .submit(ApiEndpoint::Users, "user123".to_string())
        .await
        .unwrap();
    
    println!("Result: {}", result);
}
```

## 📚 Examples

### API Request Batching

Reduce HTTP overhead by batching multiple API calls:

```rust
// Instead of 100 individual HTTP requests...
for user_id in user_ids {
    let user = api_client.get_user(user_id).await?;
}

// ...make 1 batched request!
for user_id in user_ids {
    let user = batcher.submit(ApiEndpoint::Users, user_id).await?;
}
```

### Database Query Batching

Optimize database round trips:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Table {
    Users,
    Orders,
}

struct DbClient;

#[async_trait]
impl BatchProcessor<Table, i64, Record, DbError> for DbClient {
    async fn process_batch(
        &self,
        table: Table,
        ids: Vec<i64>,
    ) -> Result<HashMap<i64, Record>, DbError> {
        // Execute: SELECT * FROM table WHERE id IN (ids)
        // Instead of N individual SELECT queries
        execute_bulk_query(table, ids).await
    }
}
```

### Blockchain Multicall Pattern

Batch RPC calls to reduce network overhead:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Chain {
    Ethereum,
    Polygon,
    Arbitrum,
}

struct BlockchainClient;

#[async_trait]
impl BatchProcessor<Chain, Address, Balance, RpcError> for BlockchainClient {
    async fn process_batch(
        &self,
        chain: Chain,
        addresses: Vec<Address>,
    ) -> Result<HashMap<Address, Balance>, RpcError> {
        // Use multicall contract to batch balance checks
        multicall_get_balances(chain, addresses).await
    }
}
```

## ⚙️ Configuration

### Batch Window

The time to wait before processing a batch:

```rust
let batcher = Batcher::new(
    processor,
    100, // 100ms - good for high-throughput APIs
    50,
);
```

**Guidelines:**
- **10-50ms**: Low-latency requirements
- **100-200ms**: Balanced performance
- **500-1000ms**: Maximum batching efficiency

### Max Batch Size

Maximum requests before immediate processing:

```rust
let batcher = Batcher::new(
    processor,
    100,
    50, // Process immediately at 50 requests
);
```

**Guidelines:**
- **10-50**: API rate limits
- **100-500**: Database bulk operations
- **1000+**: High-throughput batch processing

### Using Config Struct

```rust
use request_batcher::BatcherConfig;

let config = BatcherConfig {
    batch_window_ms: 100,
    max_batch_size: 50,
};

let batcher = Batcher::with_config(processor, config);
```

## 🔍 Monitoring

Get real-time statistics:

```rust
let stats = batcher.stats().await;
println!("Active queues: {}", stats.total_queues);
println!("Pending requests: {}", stats.total_pending_requests);
println!("Processing: {}", stats.processing_queues);
```

## 🎭 How It Works

```
Request Flow:
┌─────────────┐
│  Request 1  │──┐
└─────────────┘  │
┌─────────────┐  │    ┌──────────────┐    ┌─────────────────┐
│  Request 2  │──┼───▶│ Batch Queue  │───▶│ Batch Processor │
└─────────────┘  │    └──────────────┘    └─────────────────┘
┌─────────────┐  │           │                      │
│  Request 3  │──┘           │                      │
└─────────────┘              ▼                      ▼
                    ┌─────────────────┐    ┌─────────────────┐
                    │ Timer: 100ms    │    │ Size: 50 reqs   │
                    │ Triggers batch  │    │ Triggers batch  │
                    └─────────────────┘    └─────────────────┘
```

**Batching Logic:**
1. Requests enter resource-specific queues
2. First request starts a timer
3. Batch processes when:
   - Timer expires (time-based), OR
   - Max size reached (size-based)
4. Responses distributed to individual requesters
5. Duplicate requests automatically deduplicated

## 🧪 Testing

Run the test suite:

```bash
cargo test
```

Run examples:

```bash
# API batching example
cargo run --example api_batching

# Database batching example
cargo run --example database_batching
```

## 🎯 Performance Tips

1. **Tune batch window**: Lower for latency, higher for throughput
2. **Set appropriate max size**: Match your backend's batch limits
3. **Use separate resource types**: Isolate different APIs/tables
4. **Monitor stats**: Adjust based on queue depths
5. **Consider request size**: Large payloads may need smaller batches

## 🔒 Thread Safety

The batcher is fully thread-safe and can be cloned cheaply:

```rust
let batcher = Batcher::new(processor, 100, 50);

// Clone for use in multiple tasks
let batcher1 = batcher.clone();
let batcher2 = batcher.clone();

tokio::spawn(async move {
    batcher1.submit(resource, request).await
});

tokio::spawn(async move {
    batcher2.submit(resource, request).await
});
```

## 📖 API Documentation

Full API documentation is available at [docs.rs/request-batcher](https://docs.rs/request-batcher).

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

## 🙏 Acknowledgments

Inspired by:
- Facebook's DataLoader pattern
- GraphQL batching strategies
- Production blockchain indexing systems

## 📮 Contact

- GitHub: [@yourusername](https://github.com/yourusername)
- Issues: [GitHub Issues](https://github.com/yourusername/request-batcher/issues)

---

Made with ❤️ by the Rust community
# request_batcher
