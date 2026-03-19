# Danube Persistent Storage

`danube-persistent-storage` is the storage engine behind reliable topics in Danube.

At a high level, it gives Danube three things:

- a **local per-topic WAL** for fast writes and recent reads
- a **durable segment history** for recovery, replay, and topic moves
- **metadata-backed recovery** so offsets remain continuous across restarts and broker transfers

If you are operating Danube, you will usually use this crate indirectly through the broker `storage:` configuration rather than by embedding it yourself.

## What this crate provides

- `StorageFactory`
  - creates and recovers per-topic storage instances
- `WalStorage`
  - broker-facing persistent storage implementation for reliable topics
- `Wal`
  - the hot-path append log used for fast local writes and recent replay
- `StorageFactoryConfig`
  - storage-mode configuration for `local`, `shared_fs`, and `object_store`
- `StorageMetadata` and `SegmentDescriptor`
  - metadata abstractions used to track durable segment history

## How persistence works

Reliable-topic data flows through three layers:

1. **Hot local state**
   - new messages are appended to a local WAL
2. **Durable historical state**
   - sealed WAL history is published as immutable durable segments
3. **Metadata state**
   - segment descriptors and sealed topic state are stored in the metadata store

This lets Danube keep the write path local and efficient while still supporting:

- long-range replay
- broker restarts
- reliable topic movement
- storage backends beyond the current broker disk

## Storage modes

The crate supports the same three storage modes exposed by the broker.

### `local`

- keeps WAL and durable segments on the local filesystem
- simplest setup
- good for single-broker deployments and development

### `shared_fs`

- writes locally first
- exports durable segments to a shared filesystem
- good when multiple brokers can access the same filesystem

### `object_store`

- writes locally first
- exports durable segments to object storage via OpenDAL
- good for cloud-native multi-broker deployments

## Read behavior

Reads are tiered automatically.

- if the requested offset is still in local WAL coverage
  - data is served from WAL/cache
- if the requested offset is older than the hot local window
  - data is streamed from durable segments and then handed off to the live WAL tail

That same behavior is what allows consumers to continue reading after topic movement or local WAL cleanup.

## Topic mobility and recovery

This crate is also responsible for the persistence side of reliable topic moves.

When a topic is sealed on one broker and resumed on another:

- the old broker records the `last_committed_offset`
- durable segment metadata remains available to the new owner
- the new broker resumes at `last_committed_offset + 1`

This preserves a single continuous offset space for the topic.

## Using it in Danube

Most users should configure persistence through the broker YAML.

Typical usage looks like:

- choose `storage.mode`
  - `local`
  - `shared_fs`
  - `object_store`
- set WAL options under `storage.wal`
- set `cache_root`, `root`, or `object_store` fields depending on the selected mode

For configuration guidance, examples, and operational trade-offs, use the documentation links below.

## Using it programmatically

If you are embedding the crate directly, the main entry point is `StorageFactory`.

Typical integration flow:

1. build a `StorageFactoryConfig`
2. create a `StorageFactory` with your metadata store
3. call `for_topic("/namespace/topic")`
4. use the returned `WalStorage` as the topic’s persistent storage
5. call `seal()` during topic handoff when ownership changes

The most important exported configuration types are:

- `StorageFactoryConfig`
- `StorageMode`
- `SharedFsConfig`
- `ObjectStoreConfig`
- `RetentionConfig`
- `wal::WalConfig`

## Operational notes

- new writes always land in the local WAL first
- `shared_fs` and `object_store` use background segment export
- local WAL retention applies to staged WAL files after durable history safely covers them
- durable history uses the same framed WAL data model rather than a separate message format

## Documentation

- **Persistence & Storage**
  - <https://danube-docs.dev-state.com/concepts/persistence/>
- **Persistence Architecture**
  - <https://danube-docs.dev-state.com/architecture/persistence/>
- **Reliable Topic Moves**
  - <https://danube-docs.dev-state.com/architecture/reliable_topic_move/>
- **Broker Configuration Reference**
  - <https://danube-docs.dev-state.com/reference/broker_configuration/>

Use the documentation for configuration examples, architecture details, and implementation behavior. This README is intentionally kept at the “what it does and when to use it” level.

## Tests

Run unit and integration tests for this crate:

```bash
cargo test -p danube-persistent-storage --tests
```
