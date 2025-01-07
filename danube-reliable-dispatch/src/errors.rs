use thiserror::Error;

pub type Result<T> = std::result::Result<T, ReliableDispatchError>;

#[derive(Debug, Error)]
pub enum ReliableDispatchError {
    #[error("Lock acquisition failed: {0}")]
    LockError(String),

    #[error("Segment error: {0}")]
    SegmentError(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Acknowledgment error: {0}")]
    AcknowledgmentError(String),

    #[error("Subscription error: {0}")]
    SubscriptionError(String),

    #[error("Subscription error: {0}")]
    UnknownSubscription(String),

    #[error("Disk Storage error: {0}")]
    DiskStorageError(String),

    #[error("AWS S3 Storage error: {0}")]
    S3StorageError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Not able to get the AWS client: {0}")]
    UnknownAWSClient(String),

    #[error("AWS SDK ByteStream error: {0}")]
    ByteStreamError(#[from] aws_sdk_s3::primitives::ByteStreamError),

    #[error("AWS SDK GetObject error: {0}")]
    GetObjectError(
        #[from]
        aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_s3::operation::get_object::GetObjectError,
            aws_smithy_runtime_api::http::Response,
        >,
    ),

    #[error("AWS SDK PutObject error: {0}")]
    PutObjectError(
        #[from]
        aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_s3::operation::put_object::PutObjectError,
            aws_smithy_runtime_api::http::Response,
        >,
    ),

    #[error("AWS SDK DeleteObject error: {0}")]
    DeleteObjectError(
        #[from]
        aws_smithy_runtime_api::client::result::SdkError<
            aws_sdk_s3::operation::delete_object::DeleteObjectError,
            aws_smithy_runtime_api::http::Response,
        >,
    ),
}
