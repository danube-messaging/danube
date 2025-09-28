#[cfg(test)]
mod tests {
    use crate::{CloudStore, BackendConfig, CloudBackend, LocalBackend};
    use std::collections::HashMap;

    /// Test: Memory backend basic put/get operations
    ///
    /// Purpose
    /// - Validate basic object storage and retrieval using in-memory backend
    /// - Ensure data integrity through put/get cycle
    ///
    /// Flow
    /// - Create CloudStore with memory backend
    /// - Put test data at specific path
    /// - Get data back and verify it matches
    ///
    /// Expected
    /// - Put operation succeeds without errors
    /// - Get operation returns exact same data
    /// - No data corruption during storage/retrieval
    #[tokio::test]
    async fn test_memory_backend_put_get() {
        let store = CloudStore::new(BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: "test-prefix".to_string(),
        }).expect("create memory store");

        let test_data = b"hello world";
        let path = "test/object.bin";

        // Put object
        store.put_object(path, test_data).await.expect("put object");

        // Get object
        let retrieved = store.get_object(path).await.expect("get object");
        assert_eq!(retrieved, test_data);
    }

    /// Test: Memory backend with path prefix handling
    ///
    /// Purpose
    /// - Validate that CloudStore correctly handles path prefixes
    /// - Ensure nested paths work with memory backend
    ///
    /// Flow
    /// - Create CloudStore with specific root prefix
    /// - Store object at nested path
    /// - Retrieve object and verify data integrity
    ///
    /// Expected
    /// - Prefixed paths are handled correctly
    /// - Nested directory structures work in memory
    /// - Data retrieval works with complex paths
    #[tokio::test]
    async fn test_memory_backend_with_prefix() {
        let store = CloudStore::new(BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: "my-prefix".to_string(),
        }).expect("create memory store");

        let test_data = b"prefixed data";
        let path = "nested/path/object.bin";

        store.put_object(path, test_data).await.expect("put object");
        let retrieved = store.get_object(path).await.expect("get object");
        assert_eq!(retrieved, test_data);
    }

    /// Test: Filesystem backend object storage
    ///
    /// Purpose
    /// - Validate CloudStore filesystem backend functionality
    /// - Ensure objects are correctly stored to and read from disk
    ///
    /// Flow
    /// - Create temporary directory for testing
    /// - Create CloudStore with filesystem backend
    /// - Put/get object and verify data integrity
    ///
    /// Expected
    /// - Files are created on disk at correct paths
    /// - Data is persisted and retrievable
    /// - Filesystem operations complete successfully
    #[tokio::test]
    async fn test_filesystem_backend() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let root_path = tmp.path().to_string_lossy().to_string();

        let store = CloudStore::new(BackendConfig::Local {
            backend: LocalBackend::Fs,
            root: root_path,
        }).expect("create fs store");

        let test_data = b"filesystem test data";
        let path = "fs-test/object.bin";

        store.put_object(path, test_data).await.expect("put object");
        let retrieved = store.get_object(path).await.expect("get object");
        assert_eq!(retrieved, test_data);
    }

    /// Test: S3 backend configuration validation
    ///
    /// Purpose
    /// - Validate S3 backend configuration parsing and setup
    /// - Ensure S3 client can be created with proper credentials
    ///
    /// Flow
    /// - Create S3 configuration with MinIO-compatible settings
    /// - Attempt to create CloudStore with S3 backend
    /// - Verify configuration is accepted
    ///
    /// Expected
    /// - S3 backend configuration is parsed correctly
    /// - CloudStore creation succeeds with valid config
    /// - No errors during S3 client initialization
    #[tokio::test]
    async fn test_s3_backend_config() {
        let mut options = HashMap::new();
        options.insert("endpoint".to_string(), "http://localhost:9000".to_string());
        options.insert("region".to_string(), "us-east-1".to_string());
        options.insert("access_key".to_string(), "minioadmin".to_string());
        options.insert("secret_key".to_string(), "minioadmin".to_string());

        let result = CloudStore::new(BackendConfig::Cloud {
            backend: CloudBackend::S3,
            root: "s3://test-bucket/prefix".to_string(),
            options,
        });

        // Should create successfully (even if we can't connect)
        assert!(result.is_ok());
    }

    /// Test: Google Cloud Storage backend configuration
    ///
    /// Purpose
    /// - Validate GCS backend configuration parsing
    /// - Ensure GCS client setup works with custom endpoints
    ///
    /// Flow
    /// - Create GCS configuration with local endpoint
    /// - Attempt to create CloudStore with GCS backend
    /// - Verify configuration is accepted
    ///
    /// Expected
    /// - GCS backend configuration is parsed correctly
    /// - CloudStore creation succeeds with GCS config
    /// - Custom endpoints are handled properly
    #[tokio::test]
    async fn test_gcs_backend_config() {
        let mut options = HashMap::new();
        options.insert("endpoint".to_string(), "http://localhost:4443".to_string());

        let result = CloudStore::new(BackendConfig::Cloud {
            backend: CloudBackend::Gcs,
            root: "gcs://test-bucket/prefix".to_string(),
            options,
        });

        // Should create successfully (even if we can't connect)
        assert!(result.is_ok());
    }

    #[test]
    fn test_split_bucket_prefix_s3() {
        let result = crate::cloud_store::split_bucket_prefix("s3://my-bucket/some/prefix");
        assert!(result.is_ok());
        let (bucket, prefix) = result.unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "some/prefix");
    }

    #[test]
    fn test_split_bucket_prefix_no_prefix() {
        let result = crate::cloud_store::split_bucket_prefix("s3://my-bucket");
        assert!(result.is_ok());
        let (bucket, prefix) = result.unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_split_bucket_prefix_gcs() {
        let result = crate::cloud_store::split_bucket_prefix("gcs://another-bucket/deep/nested/prefix");
        assert!(result.is_ok());
        let (bucket, prefix) = result.unwrap();
        assert_eq!(bucket, "another-bucket");
        assert_eq!(prefix, "deep/nested/prefix");
    }

    #[test]
    fn test_split_bucket_prefix_no_scheme() {
        let result = crate::cloud_store::split_bucket_prefix("just-bucket-name");
        assert!(result.is_ok());
        let (bucket, prefix) = result.unwrap();
        assert_eq!(bucket, "just-bucket-name");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_split_bucket_prefix_empty_bucket() {
        let result = crate::cloud_store::split_bucket_prefix("s3:///prefix");
        assert!(result.is_err());
    }

    #[test]
    fn test_split_fs_root_with_scheme() {
        let result = crate::cloud_store::split_fs_root("file:///tmp/danube/storage");
        assert!(result.is_ok());
        let (root, prefix) = result.unwrap();
        assert_eq!(root, "/tmp/danube/storage");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_split_fs_root_without_scheme() {
        let result = crate::cloud_store::split_fs_root("/var/lib/danube");
        assert!(result.is_ok());
        let (root, prefix) = result.unwrap();
        assert_eq!(root, "/var/lib/danube");
        assert_eq!(prefix, "");
    }

    #[tokio::test]
    async fn test_path_joining() {
        let store = CloudStore::new(BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: "root-prefix".to_string(),
        }).expect("create memory store");

        // Test internal path joining logic through put/get
        let test_data = b"path joining test";
        
        // Various path formats should all work
        let paths = vec![
            "simple.txt",
            "/leading-slash.txt",
            "nested/path/file.txt",
            "/nested/with/leading/slash.txt",
        ];

        for path in paths {
            store.put_object(path, test_data).await.expect("put object");
            let retrieved = store.get_object(path).await.expect("get object");
            assert_eq!(retrieved, test_data, "Failed for path: {}", path);
        }
    }

    #[tokio::test]
    async fn test_nonexistent_object() {
        let store = CloudStore::new(BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: "test".to_string(),
        }).expect("create memory store");

        let result = store.get_object("nonexistent/object.bin").await;
        assert!(result.is_err(), "Should fail to get nonexistent object");
    }
}
