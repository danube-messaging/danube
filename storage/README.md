# danube-storage

Danube storage backend extensions for message storage

This is an umbrella directory for all the storage implementations that implement the [`StorageBackend`](https://github.com/danube-messaging/danube/blob/main/danube-reliable-dispatch/src/storage_backend.rs#L17) trait.

The implementations could be used as reliable message storage for the Danube cluster.

As for now the storage implementations are imported in the [danube-reliable-dispatch](https://github.com/danube-messaging/danube/tree/main/danube-reliable-dispatch) crat, but the plan is to be modular and extensible, maybe conditional compilation to make them available for the Danube users by need.

## Storage Backends

The Danube storage backend implementations are:

* In-memory - Default backend for message storage.
* [Local Disk](danube-storage-disk/)
* [AWS S3](danube-storage-s3/)

The implementations could be extended to other storage backends, like:

* Google Storage
* Azure Blob Storage
* MinIO: S3-compatible distributed object storage that can run locally
* Ceph: Distributed object storage with strong consistency guarantees
* Apache Cassandra: Excellent for storing binary data with configurable replication factor
* ScyllaDB: Compatible with Cassandra but with better performance
