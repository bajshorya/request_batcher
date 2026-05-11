use crate::error::BatchError;
use crate::processor::BatchProcessor;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};

/// A pending request waiting to be batched
#[derive(Debug)]
struct PendingRequest<Req, Res, E>
where
    Req: Send + Sync + Clone + Eq + Hash + Debug,
    Res: Send + Sync + Clone + Debug,
    E: std::error::Error + Send + Sync + Clone,
{
    request: Req,
    sender: mpsc::Sender<Result<Res, BatchError<E>>>,
}

/// Internal queue state for a specific resource type
#[derive(Debug)]
struct BatchQueue<Req, Res, E>
where
    Req: Send + Sync + Clone + Eq + Hash + Debug,
    Res: Send + Sync + Clone + Debug,
    E: std::error::Error + Send + Sync + Clone,
{
    requests: Vec<PendingRequest<Req, Res, E>>,
    timer_start: Option<Instant>,
    processing: bool,
}

impl<Req, Res, E> BatchQueue<Req, Res, E>
where
    Req: Send + Sync + Clone + Eq + Hash + Debug,
    Res: Send + Sync + Clone + Debug,
    E: std::error::Error + Send + Sync + Clone,
{
    fn new() -> Self {
        Self {
            requests: Vec::new(),
            timer_start: None,
            processing: false,
        }
    }
}

/// Configuration for the batcher
#[derive(Debug, Clone)]
pub struct BatcherConfig {
    /// Time window in milliseconds to wait before processing a batch
    pub batch_window_ms: u64,
    /// Maximum number of requests in a batch before processing immediately
    pub max_batch_size: usize,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            batch_window_ms: 100,
            max_batch_size: 50,
        }
    }
}

/// The main request batcher
///
/// Batches requests by resource type, using time windows and size limits to
/// determine when to process batches.
///
/// # Type Parameters
///
/// * `R` - Resource type (must be `Hash + Eq` for queue separation)
/// * `Req` - Request type (must be `Hash + Eq` for deduplication)
/// * `Res` - Response type
/// * `E` - Error type
/// * `P` - Processor type implementing `BatchProcessor`
pub struct Batcher<R, Req, Res, E, P>
where
    R: Send + Sync + Clone + Eq + Hash + Debug + 'static,
    Req: Send + Sync + Clone + Eq + Hash + Debug + 'static,
    Res: Send + Sync + Clone + Debug + 'static,
    E: std::error::Error + Send + Sync + Clone + 'static,
    P: BatchProcessor<R, Req, Res, E> + 'static,
{
    queues: Arc<tokio::sync::Mutex<HashMap<R, BatchQueue<Req, Res, E>>>>,
    config: BatcherConfig,
    processor: Arc<P>,
}

impl<R, Req, Res, E, P> Clone for Batcher<R, Req, Res, E, P>
where
    R: Send + Sync + Clone + Eq + Hash + Debug + 'static,
    Req: Send + Sync + Clone + Eq + Hash + Debug + 'static,
    Res: Send + Sync + Clone + Debug + 'static,
    E: std::error::Error + Send + Sync + Clone + 'static,
    P: BatchProcessor<R, Req, Res, E> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            queues: self.queues.clone(),
            config: self.config.clone(),
            processor: self.processor.clone(),
        }
    }
}

impl<R, Req, Res, E, P> Batcher<R, Req, Res, E, P>
where
    R: Send + Sync + Clone + Eq + Hash + Debug + 'static,
    Req: Send + Sync + Clone + Eq + Hash + Debug + 'static,
    Res: Send + Sync + Clone + Debug + 'static,
    E: std::error::Error + Send + Sync + Clone + 'static,
    P: BatchProcessor<R, Req, Res, E> + 'static,
{
    /// Create a new batcher with a custom processor
    ///
    /// # Arguments
    ///
    /// * `processor` - The batch processor implementation
    /// * `batch_window_ms` - Time window in milliseconds
    /// * `max_batch_size` - Maximum batch size
    pub fn new(processor: Arc<P>, batch_window_ms: u64, max_batch_size: usize) -> Self {
        Self {
            queues: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            config: BatcherConfig {
                batch_window_ms,
                max_batch_size,
            },
            processor,
        }
    }

    /// Create a new batcher with a config struct
    pub fn with_config(processor: Arc<P>, config: BatcherConfig) -> Self {
        Self {
            queues: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            config,
            processor,
        }
    }

    /// Submit a request to be batched
    ///
    /// This method will either:
    /// - Add the request to an existing batch and wait for the time window
    /// - Trigger immediate processing if the batch size limit is reached
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource type for this request
    /// * `request` - The individual request data
    ///
    /// # Returns
    ///
    /// The response for this specific request, or an error
    pub async fn submit(&self, resource: R, request: Req) -> Result<Res, BatchError<E>> {
        let (tx, mut rx) = mpsc::channel(1);

        let mut queues = self.queues.lock().await;
        let queue = queues.entry(resource.clone()).or_insert_with(BatchQueue::new);

        // Check if we should process immediately
        let should_process_immediately = queue.requests.len() + 1 >= self.config.max_batch_size;

        queue.requests.push(PendingRequest {
            request: request.clone(),
            sender: tx,
        });

        let batch_window_ms = self.config.batch_window_ms;
        let queues_clone = self.queues.clone();
        let processor_clone = self.processor.clone();

        // Start timer if this is the first request
        if queue.timer_start.is_none() {
            queue.timer_start = Some(Instant::now());
            let resource_clone = resource.clone();
            let queues_timer = queues_clone.clone();
            let processor_timer = processor_clone.clone();

            tokio::spawn(async move {
                sleep(Duration::from_millis(batch_window_ms)).await;

                let mut queues = queues_timer.lock().await;
                if let Some(queue) = queues.get_mut(&resource_clone) {
                    if !queue.processing && !queue.requests.is_empty() {
                        queue.processing = true;
                        let requests = std::mem::take(&mut queue.requests);
                        queue.timer_start = None;
                        drop(queues);

                        // Process batch
                        Self::process_batch_internal(
                            resource_clone.clone(),
                            requests,
                            processor_timer,
                        )
                        .await;

                        let mut queues = queues_timer.lock().await;
                        if let Some(queue) = queues.get_mut(&resource_clone) {
                            queue.processing = false;
                        }
                    }
                }
            });
        }

        // Process immediately if max batch size reached
        if should_process_immediately {
            queue.processing = true;
            let requests = std::mem::take(&mut queue.requests);
            queue.timer_start = None;
            drop(queues);

            Self::process_batch_internal(resource.clone(), requests, processor_clone).await;

            let mut queues = self.queues.lock().await;
            if let Some(queue) = queues.get_mut(&resource) {
                queue.processing = false;
            }
        } else {
            drop(queues);
        }

        // Wait for response
        rx.recv()
            .await
            .ok_or(BatchError::ChannelClosed)?
    }

    /// Internal batch processing logic
    async fn process_batch_internal(
        resource: R,
        pending_requests: Vec<PendingRequest<Req, Res, E>>,
        processor: Arc<P>,
    ) {
        if pending_requests.is_empty() {
            return;
        }

        // Extract unique requests (deduplication)
        let mut unique_requests = Vec::new();
        let mut seen = HashSet::new();
        
        for pending in &pending_requests {
            if seen.insert(pending.request.clone()) {
                unique_requests.push(pending.request.clone());
            }
        }

        // Process the batch
        let result = processor.process_batch(resource, unique_requests).await;

        // Distribute results to all requesters
        match result {
            Ok(response_map) => {
                for pending in pending_requests {
                    if let Some(response) = response_map.get(&pending.request) {
                        let _ = pending.sender.send(Ok(response.clone())).await;
                    } else {
                        let _ = pending.sender.send(Err(BatchError::RequestNotFound)).await;
                    }
                }
            }
            Err(e) => {
                // Send the same error to all requesters
                let error = BatchError::processing_failed(e);
                for pending in pending_requests {
                    let _ = pending.sender.send(Err(error.clone())).await;
                }
            }
        }
    }

    /// Get current statistics about the batcher
    pub async fn stats(&self) -> BatcherStats {
        let queues = self.queues.lock().await;
        let total_queues = queues.len();
        let total_pending: usize = queues.values().map(|q| q.requests.len()).sum();
        let processing_queues = queues.values().filter(|q| q.processing).count();

        BatcherStats {
            total_queues,
            total_pending_requests: total_pending,
            processing_queues,
        }
    }
}

/// Statistics about the current state of the batcher
#[derive(Debug, Clone)]
pub struct BatcherStats {
    /// Number of resource queues currently active
    pub total_queues: usize,
    /// Total number of pending requests across all queues
    pub total_pending_requests: usize,
    /// Number of queues currently processing
    pub processing_queues: usize,
}
