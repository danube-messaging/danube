use futures::stream::Stream;
use futures::StreamExt;
use std::task::{Context, Poll};
use std::{fmt, pin::Pin};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use super::errors::{MetadataError, Result};

#[derive(Debug, Clone)]
pub enum WatchEvent {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        mod_revision: Option<i64>,
        version: Option<i64>,
    },
    Delete {
        key: Vec<u8>,
        mod_revision: Option<i64>,
        version: Option<i64>,
    },
}

pub struct WatchStream {
    inner: Pin<Box<dyn Stream<Item = Result<WatchEvent>> + Send>>,
}

impl Stream for WatchStream {
    type Item = Result<WatchEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl WatchStream {
    pub fn new(stream: impl Stream<Item = Result<WatchEvent>> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(stream),
        }
    }
    /// Create a WatchStream from a `tokio::sync::broadcast::Receiver`.
    /// Broadcast lag (slow consumer) is surfaced as `MetadataError::WatchError`.
    pub fn from_broadcast(rx: broadcast::Receiver<WatchEvent>) -> Self {
        let stream = BroadcastStream::new(rx).filter_map(|result| {
            futures::future::ready(match result {
                Ok(event) => Some(Ok(event)),
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    Some(Err(MetadataError::WatchError(format!(
                        "watch lagged by {} events — consumer should resync",
                        n
                    ))))
                }
            })
        });
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl fmt::Display for WatchEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WatchEvent::Put {
                key,
                value: _,
                mod_revision,
                version,
            } => {
                let key_str = String::from_utf8_lossy(key);
                write!(f, "Put(key: {}", key_str)?;
                if let Some(rev) = mod_revision {
                    write!(f, ", mod_revision: {}", rev)?;
                }
                if let Some(ver) = version {
                    write!(f, ", version: {}", ver)?;
                }
                write!(f, ")")
            }
            WatchEvent::Delete {
                key,
                mod_revision,
                version,
            } => {
                let key_str = String::from_utf8_lossy(key);
                write!(f, "Delete(key: {}", key_str)?;
                if let Some(rev) = mod_revision {
                    write!(f, ", mod_revision: {}", rev)?;
                }
                if let Some(ver) = version {
                    write!(f, ", version: {}", ver)?;
                }
                write!(f, ")")
            }
        }
    }
}
