# danube-metadata-store

This danube-metadata-store aims to reduce the hard dependency on ETCD for metadata storage by providing an abstraction layer that allows different storage systems to be used interchangeably for managing metadata. It offers a unified interface for operations such as get, put, and watch across various backend implementations.
