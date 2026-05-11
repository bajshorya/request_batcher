use async_trait::async_trait;
use request_batcher::{BatchProcessor, Batcher};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TestResource {
    TypeA,
    TypeB,
}

#[derive(Debug, Clone)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

struct TestProcessor;

#[async_trait]
impl BatchProcessor<TestResource, String, String, TestError> for TestProcessor {
    async fn process_batch(
        &self,
        _resource: TestResource,
        requests: Vec<String>,
    ) -> Result<HashMap<String, String>, TestError> {
        let mut results = HashMap::new();
        for req in requests {
            results.insert(req.clone(), format!("Response for {}", req));
        }
        Ok(results)
    }
}

#[tokio::test]
async fn test_basic_batching() {
    let processor = Arc::new(TestProcessor);
    let batcher = Batcher::new(processor, 100, 10);

    let result = batcher
        .submit(TestResource::TypeA, "test1".to_string())
        .await
        .unwrap();

    assert_eq!(result, "Response for test1");
}

#[tokio::test]
async fn test_concurrent_requests_batched() {
    let processor = Arc::new(TestProcessor);
    let batcher = Batcher::new(processor, 100, 10);

    let mut handles = vec![];
    for i in 0..5 {
        let batcher_clone = batcher.clone();
        let handle = tokio::spawn(async move {
            batcher_clone
                .submit(TestResource::TypeA, format!("req{}", i))
                .await
                .unwrap()
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 5);
    for (i, result) in results.iter().enumerate() {
        assert_eq!(result, &format!("Response for req{}", i));
    }
}

#[tokio::test]
async fn test_size_based_trigger() {
    struct CountingProcessor {
        batch_count: Arc<tokio::sync::Mutex<usize>>,
    }

    #[async_trait]
    impl BatchProcessor<TestResource, String, String, TestError> for CountingProcessor {
        async fn process_batch(
            &self,
            _resource: TestResource,
            requests: Vec<String>,
        ) -> Result<HashMap<String, String>, TestError> {
            let mut count = self.batch_count.lock().await;
            *count += 1;
            
            let mut results = HashMap::new();
            for req in requests {
                results.insert(req.clone(), format!("Response for {}", req));
            }
            Ok(results)
        }
    }

    let batch_count = Arc::new(tokio::sync::Mutex::new(0));
    let processor = Arc::new(CountingProcessor {
        batch_count: batch_count.clone(),
    });
    
    let batcher = Batcher::new(
        processor,
        5000, // Very long window
        3,    // Small batch size
    );

    // Submit 3 requests - should trigger immediate batch
    let mut handles = vec![];
    for i in 0..3 {
        let batcher_clone = batcher.clone();
        let handle = tokio::spawn(async move {
            batcher_clone
                .submit(TestResource::TypeA, format!("req{}", i))
                .await
                .unwrap()
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    let count = *batch_count.lock().await;
    assert_eq!(count, 1, "Should have processed exactly 1 batch");
}

#[tokio::test]
async fn test_separate_resource_queues() {
    struct ResourceTrackingProcessor {
        calls: Arc<tokio::sync::Mutex<Vec<TestResource>>>,
    }

    #[async_trait]
    impl BatchProcessor<TestResource, String, String, TestError> for ResourceTrackingProcessor {
        async fn process_batch(
            &self,
            resource: TestResource,
            requests: Vec<String>,
        ) -> Result<HashMap<String, String>, TestError> {
            self.calls.lock().await.push(resource);
            
            let mut results = HashMap::new();
            for req in requests {
                results.insert(req.clone(), format!("Response for {}", req));
            }
            Ok(results)
        }
    }

    let calls = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let processor = Arc::new(ResourceTrackingProcessor {
        calls: calls.clone(),
    });
    
    let batcher = Batcher::new(processor, 50, 10);

    // Submit to different resources
    let h1 = {
        let b = batcher.clone();
        tokio::spawn(async move {
            b.submit(TestResource::TypeA, "a1".to_string()).await.unwrap()
        })
    };
    
    let h2 = {
        let b = batcher.clone();
        tokio::spawn(async move {
            b.submit(TestResource::TypeB, "b1".to_string()).await.unwrap()
        })
    };

    h1.await.unwrap();
    h2.await.unwrap();

    // Wait for batches to process
    sleep(Duration::from_millis(100)).await;

    let processed_calls = calls.lock().await;
    assert_eq!(processed_calls.len(), 2, "Should have 2 separate batches");
    assert!(processed_calls.contains(&TestResource::TypeA));
    assert!(processed_calls.contains(&TestResource::TypeB));
}

#[tokio::test]
async fn test_deduplication() {
    struct DeduplicationProcessor;

    #[async_trait]
    impl BatchProcessor<TestResource, String, String, TestError> for DeduplicationProcessor {
        async fn process_batch(
            &self,
            _resource: TestResource,
            requests: Vec<String>,
        ) -> Result<HashMap<String, String>, TestError> {
            // Verify no duplicates in the batch
            let unique_count = requests.iter().collect::<std::collections::HashSet<_>>().len();
            assert_eq!(
                unique_count,
                requests.len(),
                "Batch should not contain duplicates"
            );
            
            let mut results = HashMap::new();
            for req in requests {
                results.insert(req.clone(), format!("Response for {}", req));
            }
            Ok(results)
        }
    }

    let processor = Arc::new(DeduplicationProcessor);
    let batcher = Batcher::new(processor, 100, 10);

    // Submit duplicate requests
    let mut handles = vec![];
    for _ in 0..3 {
        let batcher_clone = batcher.clone();
        let handle = tokio::spawn(async move {
            batcher_clone
                .submit(TestResource::TypeA, "duplicate".to_string())
                .await
                .unwrap()
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // All should get the same response
    assert_eq!(results.len(), 3);
    for result in results {
        assert_eq!(result, "Response for duplicate");
    }
}

#[tokio::test]
async fn test_stats() {
    let processor = Arc::new(TestProcessor);
    let batcher = Batcher::new(processor, 5000, 100); // Long window to keep requests pending

    // Submit some requests but don't await them yet
    let h1 = {
        let b = batcher.clone();
        tokio::spawn(async move {
            b.submit(TestResource::TypeA, "req1".to_string()).await
        })
    };

    // Give it a moment to queue
    sleep(Duration::from_millis(10)).await;

    let stats = batcher.stats().await;
    assert!(stats.total_queues > 0);

    // Clean up
    h1.await.unwrap().unwrap();
}
