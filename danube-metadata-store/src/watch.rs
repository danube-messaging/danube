use etcd_client::{EventType, WatchStream as EtcdWatchStream};
use futures::stream::Stream;
use futures::StreamExt;
use std::task::{Context, Poll};
use std::{fmt, pin::Pin};

use crate::errors::{MetadataError, Result};

#[derive(Debug)]
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
    pub(crate) fn from_etcd(stream: EtcdWatchStream) -> Self {
        let stream = stream.flat_map(|result| {
            futures::stream::iter(
                result
                    .map_err(MetadataError::from)
                    .and_then(|watch_response| {
                        Ok(watch_response
                            .events()
                            .iter()
                            .map(|event| {
                                let key_value = event.kv().unwrap();
                                match event.event_type() {
                                    EventType::Put => Ok(WatchEvent::Put {
                                        key: key_value.key().to_vec(),
                                        value: key_value.value().to_vec(),
                                        mod_revision: Some(key_value.mod_revision()),
                                        version: Some(key_value.version()),
                                    }),
                                    EventType::Delete => Ok(WatchEvent::Delete {
                                        key: key_value.key().to_vec(),
                                        mod_revision: Some(key_value.mod_revision()),
                                        version: Some(key_value.version()),
                                    }),
                                }
                            })
                            .collect::<Vec<_>>())
                    })
                    .into_iter()
                    .flatten(),
            )
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
