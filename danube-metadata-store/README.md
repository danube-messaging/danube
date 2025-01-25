# danube-metadata-store

This crate is responsible for storing and retrieving metadata for the Danube messaging broker.

ETCD is used as a coordination service within the Danube distributed system. It uses the built-in Watch mechanism to monitor specific keys or directories for changes and react to those changes.

The danube-metadata-store also provides an abstraction layer that offers a unified interface for operations such as get, put, and watch. This allows to use different storage system implementations for managing metadata, if needed.
