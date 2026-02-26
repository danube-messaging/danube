//! Persistent Raft log storage backed by `redb`.
//!
//! Stores Raft log entries, vote, and purge state in a local `redb` database.
//! This ensures Raft correctness across broker restarts.

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, Vote};
use tokio::sync::Mutex;
use tracing::debug;

use redb::{Database, ReadableTable, TableDefinition};

use crate::typ::TypeConfig;

type NodeId = u64;

// redb table definitions
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const VOTE_KEY: &str = "vote";
const PURGED_KEY: &str = "last_purged";
const COMMITTED_KEY: &str = "committed";

/// Persistent Raft log store using redb.
pub struct RedbLogStore {
    db: Arc<redb::Database>,
    /// In-memory cache of last purged log id for fast access.
    last_purged: Mutex<Option<LogId<NodeId>>>,
}

impl RedbLogStore {
    /// Open or create the log store at the given path.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError<NodeId>> {
        let db = Database::create(path).map_err(|e| to_storage_err(e, "open database"))?;

        // Create tables if they don't exist
        let write_txn = db
            .begin_write()
            .map_err(|e| to_storage_err(e, "begin write txn"))?;
        {
            let _ = write_txn
                .open_table(LOG_TABLE)
                .map_err(|e| to_storage_err(e, "create log table"))?;
            let _ = write_txn
                .open_table(META_TABLE)
                .map_err(|e| to_storage_err(e, "create meta table"))?;
        }
        write_txn
            .commit()
            .map_err(|e| to_storage_err(e, "commit init"))?;

        // Read last_purged from disk
        let last_purged = {
            let read_txn = db
                .begin_read()
                .map_err(|e| to_storage_err(e, "begin read txn"))?;
            let table = read_txn
                .open_table(META_TABLE)
                .map_err(|e| to_storage_err(e, "open meta table"))?;
            match table.get(PURGED_KEY) {
                Ok(Some(val)) => {
                    let bytes = val.value();
                    Some(
                        bincode::deserialize::<LogId<NodeId>>(bytes)
                            .map_err(|e| to_storage_err(e, "deserialize last_purged"))?,
                    )
                }
                _ => None,
            }
        };

        Ok(Self {
            db: Arc::new(db),
            last_purged: Mutex::new(last_purged),
        })
    }

    fn read_meta<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StorageError<NodeId>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| to_storage_err(e, "begin read txn"))?;
        let table = read_txn
            .open_table(META_TABLE)
            .map_err(|e| to_storage_err(e, "open meta table"))?;
        match table.get(key) {
            Ok(Some(val)) => {
                let bytes = val.value();
                let v = bincode::deserialize(bytes)
                    .map_err(|e| to_storage_err(e, "deserialize meta"))?;
                Ok(Some(v))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(to_storage_err(e, "read meta")),
        }
    }

    fn write_meta<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = bincode::serialize(value).map_err(|e| to_storage_err(e, "serialize meta"))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| to_storage_err(e, "begin write txn"))?;
        {
            let mut table = write_txn
                .open_table(META_TABLE)
                .map_err(|e| to_storage_err(e, "open meta table"))?;
            table
                .insert(key, bytes.as_slice())
                .map_err(|e| to_storage_err(e, "insert meta"))?;
        }
        write_txn
            .commit()
            .map_err(|e| to_storage_err(e, "commit meta"))?;
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for RedbLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| to_storage_err(e, "begin read txn"))?;
        let table = read_txn
            .open_table(LOG_TABLE)
            .map_err(|e| to_storage_err(e, "open log table"))?;

        let mut entries = Vec::new();
        let iter = table
            .range(range)
            .map_err(|e| to_storage_err(e, "range query"))?;
        for item in iter {
            let (_key, val) = item.map_err(|e| to_storage_err(e, "iterate log"))?;
            let entry: Entry<TypeConfig> = serde_json::from_slice(val.value())
                .map_err(|e| to_storage_err(e, "deserialize entry"))?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for RedbLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged = self.last_purged.lock().await.clone();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| to_storage_err(e, "begin read txn"))?;
        let table = read_txn
            .open_table(LOG_TABLE)
            .map_err(|e| to_storage_err(e, "open log table"))?;

        let last_log_id = match table.last() {
            Ok(Some((_key, val))) => {
                let entry: Entry<TypeConfig> = serde_json::from_slice(val.value())
                    .map_err(|e| to_storage_err(e, "deserialize last entry"))?;
                Some(entry.log_id)
            }
            _ => last_purged,
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Return a new RedbLogStore sharing the same db handle.
        Self {
            db: self.db.clone(),
            last_purged: Mutex::new(self.last_purged.lock().await.clone()),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.write_meta(VOTE_KEY, vote)?;
        debug!(?vote, "vote saved");
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.read_meta(VOTE_KEY)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| to_storage_err(e, "begin write txn"))?;
        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|e| to_storage_err(e, "open log table"))?;
            for entry in entries {
                let bytes =
                    serde_json::to_vec(&entry).map_err(|e| to_storage_err(e, "serialize entry"))?;
                table
                    .insert(entry.log_id.index, bytes.as_slice())
                    .map_err(|e| to_storage_err(e, "insert entry"))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| to_storage_err(e, "commit append"))?;

        // Entries are persisted on disk after commit — invoke callback.
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| to_storage_err(e, "begin write txn"))?;
        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|e| to_storage_err(e, "open log table"))?;

            // Remove all entries from log_id.index onwards (inclusive)
            let keys_to_remove: Vec<u64> = table
                .range(log_id.index..)
                .map_err(|e| to_storage_err(e, "range for truncate"))?
                .map(|item| item.map(|(k, _)| k.value()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| to_storage_err(e, "collect truncate keys"))?;

            for key in keys_to_remove {
                table
                    .remove(key)
                    .map_err(|e| to_storage_err(e, "remove entry"))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| to_storage_err(e, "commit truncate"))?;

        debug!(?log_id, "log truncated");
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| to_storage_err(e, "begin write txn"))?;
        {
            let mut table = write_txn
                .open_table(LOG_TABLE)
                .map_err(|e| to_storage_err(e, "open log table"))?;

            // Remove all entries up to and including log_id.index
            let keys_to_remove: Vec<u64> = table
                .range(..=log_id.index)
                .map_err(|e| to_storage_err(e, "range for purge"))?
                .map(|item| item.map(|(k, _)| k.value()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| to_storage_err(e, "collect purge keys"))?;

            for key in keys_to_remove {
                table
                    .remove(key)
                    .map_err(|e| to_storage_err(e, "remove entry"))?;
            }

            // Persist last_purged in meta table
            let meta_bytes = bincode::serialize(&log_id)
                .map_err(|e| to_storage_err(e, "serialize last_purged"))?;
            let mut meta = write_txn
                .open_table(META_TABLE)
                .map_err(|e| to_storage_err(e, "open meta table"))?;
            meta.insert(PURGED_KEY, meta_bytes.as_slice())
                .map_err(|e| to_storage_err(e, "insert last_purged"))?;
        }
        write_txn
            .commit()
            .map_err(|e| to_storage_err(e, "commit purge"))?;

        *self.last_purged.lock().await = Some(log_id);
        debug!(?log_id, "log purged");
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(c) = &committed {
            self.write_meta(COMMITTED_KEY, c)?;
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        self.read_meta(COMMITTED_KEY)
    }
}

/// Convert any error into a StorageError for openraft.
fn to_storage_err(e: impl std::error::Error + 'static, context: &str) -> StorageError<NodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        std::io::Error::new(std::io::ErrorKind::Other, format!("{}: {}", context, e)),
    )
}
