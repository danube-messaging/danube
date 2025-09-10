Danube Technical Documentation: Iceberg-Native Storage Architecture


I. Introduction & Guiding Principles

This document outlines the technical specification for re-architecting the Danube messaging platform's persistence layer. The primary goal is to evolve Danube into a cloud-native, highly reliable, and scalable pub/sub platform by leveraging modern data lakehouse technologies, specifically Apache Iceberg and cloud object storage like Amazon S3.

This is not a pivot to an analytics platform. Instead, this architecture uses the robust, transactional capabilities of Iceberg to provide superior durability, consistency, and scalability for Danube's core messaging function.

The architecture is founded on three key principles:

Disaggregation of Compute and Storage: Broker nodes will be stateless, handling message processing and routing. All persistent data will reside in a shared object storage layer, enabling independent and elastic scaling of compute resources.1

Low-Latency Writes via Write-Ahead Log (WAL): To meet the demands of a high-performance messaging system, a WAL will be used to provide fast, durable acknowledgments to producers, decoupling producer latency from the higher latency of object storage.3

Transactional Durability with Apache Iceberg: All message data will be stored in an open, transactional table format. This provides ACID guarantees, safe concurrency, and robust data management features directly on the storage layer.

II. Core Concepts & Architectural Rationale

This section clarifies the "why" behind the core technology choices, addressing key questions about the new architecture.


The Role of Apache Iceberg

Apache Iceberg is not an intermediary service or a database. It is an open table format—a specification for how to organize data files (e.g., Parquet) and metadata files in object storage to make them behave like a reliable, transactional SQL table.

Using Iceberg over writing files directly to S3 with the AWS SDK provides critical features for a durable messaging platform:

Atomic Commits: A batch of messages may be written as multiple data files. Iceberg ensures that this entire batch is committed in a single, atomic operation by updating a single metadata pointer. This prevents consumers from ever seeing partial or incomplete writes, even if a broker crashes mid-operation.

Safe Concurrency: Iceberg uses optimistic concurrency control to manage simultaneous writes from multiple brokers to the same topic. This prevents data corruption and race conditions, ensuring a linear, serializable history of commits.

Schema Evolution: Message schemas can change over time. Iceberg provides robust support for schema evolution (adding, dropping, renaming fields) without rewriting data or breaking existing consumers, which is a complex problem to solve with raw files.

Performance: Iceberg's metadata layer contains statistics about the data in each file. This allows consumers to efficiently find the data they need without performing slow and expensive "list" operations on potentially millions of files in an S3 bucket.


The Role of an Iceberg Catalog (AWS Glue vs. etcd)

While Danube already uses etcd, an Iceberg Catalog serves a distinct and vital purpose.5

etcd's Role (Operational State): etcd will continue to be the source of truth for the operational state of the Danube cluster. This includes service discovery, topic ownership, broker coordination, and, in the new architecture, tracking consumer subscription progress (the last processed snapshot ID).

Iceberg Catalog's Role (Data State): The Iceberg Catalog's sole job is to manage the state of the data itself. It provides the atomic "compare-and-swap" mechanism required to commit a new version of a table. When a broker writes new data, it tells the catalog, "I'm updating the table from version X to version Y." The catalog ensures this update only succeeds if the current version is still X. This is the core mechanism that enables Iceberg's ACID guarantees and safe concurrency.7

For production, AWS Glue is a managed, serverless implementation of this catalog.8 For local testing, a lightweight

REST Catalog can be used to provide the same API.


The Write-Ahead Log (WAL) Pattern

The WAL is not a feature of Apache Iceberg. It is a well-established architectural pattern that we will introduce into Danube to solve the latency challenge of object storage.

Danube's reliable dispatch requires a "store-then-notify" semantic.10 Writing directly to S3 and committing to Iceberg for every message batch would introduce hundreds of milliseconds of latency, which is unacceptable. The WAL resolves this by:

Storing: The message is first written to a durable, low-latency log on a local block device (like an AWS EBS volume). This is an extremely fast operation.12

Notifying: As soon as the WAL write is confirmed, an acknowledgment is sent to the producer.

Forwarding: A background process then asynchronously moves the data from the WAL to its final destination in an Iceberg table on S3.

III. Implementation Plan & Crate Structure

The new functionality will be encapsulated within a new crate, minimizing disruption to the existing Danube codebase.


New Crate: danube-iceberg-storage

A new Rust crate, danube-iceberg-storage, will be created. It will implement the PersistentStorage trait required by danube-reliable-dispatch.


New Dependencies (Cargo.toml)

Crate Name	Purpose
iceberg-rust	Core Apache Iceberg SDK for interacting with catalogs and tables.
arrow-rs	In-memory columnar data representation (RecordBatch).
parquet	Reading and writing Apache Parquet files.
object_store	A unified, asynchronous API for cloud storage (S3, MinIO).
tokio	The asynchronous runtime for managing concurrent tasks.

High-Level Structure and Responsibilities


danube-broker

Role: No major changes. It will initialize either the existing storage backend or the new IcebergStorage based on the danube_broker.yml configuration.


danube-reliable-dispatch

Role: The core logic of "store-then-notify" is preserved.10

TopicStore Struct: Its interaction with the persistence layer will be simplified. When a message arrives, its store method will now make a single, fast, synchronous call to the IcebergStorage's WAL. The existing TopicCache can be repurposed to cache hot data read from S3 by the new TopicReader for low-latency tailing reads.


danube-iceberg-storage (New Crate)

This crate will contain the primary logic for the new storage backend.

struct IcebergStorage

Purpose: The public entry point for the crate. Implements the PersistentStorage trait.

Methods:

async fn store(&self, topic: &str, messages: Vec<Message>) -> Result<()>: Appends messages to the topic-specific WAL file and returns immediately upon successful fsync.

struct WriteAheadLog

Purpose: Manages a durable, append-only log file on a local disk.

Methods:

async fn append(&mut self, data: &[u8]) -> Result<u64>: Writes data to the log file and ensures it is flushed to disk. Returns the offset.

async fn read_from(&self, offset: u64) -> Result<Vec<u8>>: Reads data from a given offset, used by the TopicWriter.

struct TopicWriter (async task)

Purpose: A long-running background task, one per active topic. It moves data from the WAL to Iceberg on S3.

Logic:

Continuously tails the WAL for its topic.

Batches messages in memory into an Arrow RecordBatch.

When a size or time threshold is met, it triggers a flush.

async fn flush_and_commit(&mut self):

Serializes the RecordBatch to Parquet format.

Uploads the Parquet file to a unique path in S3.

Creates a new Iceberg snapshot by committing the new data file's location to the Iceberg Catalog.

struct TopicReader (async task)

Purpose: A long-running background task, one per active subscription. It streams data from the Iceberg table to a consumer.

Logic:

Periodically polls the Iceberg Catalog for the latest snapshot ID of the topic's table.

Compares the latest snapshot ID with the last-processed ID for its subscription (stored in etcd).

If new snapshots exist, it performs an incremental scan to identify the new data files.

Fetches the new Parquet files from S3, deserializes the messages, and sends them to the consumer.

IV. Write and Read Path Logic


The Write Path: Low-Latency Ingestion

Ingestion: A producer sends a message to a Danube broker.

Reliable Dispatch: danube-reliable-dispatch receives the message and calls the store method on the IcebergStorage implementation.

WAL Commit: The IcebergStorage instance immediately appends the message to the WriteAheadLog file on its local persistent disk (e.g., an EBS volume). An fsync call ensures the data is durable.

Producer ACK: As soon as the WAL write is confirmed, the store method returns success, and an acknowledgment is sent back to the producer. This entire process is very fast.

Asynchronous Flush to S3: In the background, the TopicWriter task for that topic reads the new message from the WAL and adds it to an in-memory batch.

Iceberg Commit: When the batch is full or a timer expires, the TopicWriter writes the batch as a new Parquet file to S3. It then performs an atomic commit to the Iceberg Catalog, which creates a new table snapshot, making the data officially part of the topic's history.


The Read Path: Streaming from Snapshots

Subscription: A consumer connects and subscribes to a topic.

Cursor Retrieval: The broker queries etcd to find the last successfully processed Iceberg snapshot ID for that subscription. If none exists, it starts from the current latest snapshot.

Polling for Data: The TopicReader task for the subscription begins periodically polling the Iceberg Catalog to check for a new latest snapshot ID.13

Incremental Scan: When the TopicReader detects a new snapshot, it asks the Iceberg API for an incremental scan—a list of all data files that were added between the last processed snapshot and the new latest one.

Data Retrieval & Delivery: The TopicReader fetches only these new Parquet files from S3, deserializes the messages, and streams them to the consumer.

Cursor Commit: Once the consumer acknowledges the messages, the broker updates the subscription's last processed snapshot ID in etcd.