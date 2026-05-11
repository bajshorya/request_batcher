use thiserror::Error;

/// Errors that can occur during batch processing
#[derive(Error, Debug, Clone)]
pub enum BatchError<E: std::error::Error + Clone> {
    /// The batch processing failed with a custom error
    #[error("Batch processing failed: {0}")]
    ProcessingFailed(E),

    /// The response channel was closed unexpectedly
    #[error("Response channel closed")]
    ChannelClosed,

    /// The request was not found in the batch results
    #[error("Request not found in batch results")]
    RequestNotFound,

    /// A generic error occurred
    #[error("An error occurred: {0}")]
    Other(String),
}

impl<E: std::error::Error + Clone> BatchError<E> {
    pub fn processing_failed(error: E) -> Self {
        Self::ProcessingFailed(error)
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::Other(message.into())
    }
}
