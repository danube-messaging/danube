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

    /// Test: split_bucket_prefix() table-driven coverage
    ///
    /// Purpose
    /// - Consolidate multiple parsing cases for bucket/prefix URIs
    /// - Ensure both S3/GCS schemes and scheme-less inputs are handled
    ///
    /// Flow
    /// - Iterate through a table of (input, expected or error)
    /// - Assert parsed (bucket, prefix) or error accordingly
    ///
    /// Expected
    /// - Correct bucket/prefix pairs for valid inputs
    /// - Error for invalid inputs (e.g., empty bucket)
    #[test]
    fn test_split_bucket_prefix_table() {
        let cases: Vec<(&str, Option<(&str, &str)>)> = vec![
            ("s3://my-bucket/some/prefix", Some(("my-bucket", "some/prefix"))),
            ("s3://my-bucket",              Some(("my-bucket", ""))),
            ("gcs://another-bucket/deep/nested/prefix", Some(("another-bucket", "deep/nested/prefix"))),
            ("just-bucket-name",            Some(("just-bucket-name", ""))),
            ("s3:///prefix",                None),
        ];

        for (input, expected) in cases {
            let res = crate::cloud_store::split_bucket_prefix(input);
            match expected {
                Some((eb, ep)) => {
                    let (b, p) = res.expect("expected Ok");
                    assert_eq!(b, eb);
                    assert_eq!(p, ep);
                }
                None => {
                    assert!(res.is_err(), "expected Err for input: {}", input);
                }
            }
        }
    }

    /// Test: split_fs_root() table-driven coverage
    ///
    /// Purpose
    /// - Consolidate filesystem root parsing cases (with/without file://)
    ///
    /// Flow
    /// - Iterate through a table of (input, expected_root, expected_prefix)
    ///
    /// Expected
    /// - Correct absolute root path and empty prefix for supported inputs
    #[test]
    fn test_split_fs_root_table() {
        let cases: Vec<(&str, &str, &str)> = vec![
            ("file:///tmp/danube/storage", "/tmp/danube/storage", ""),
            ("/var/lib/danube",            "/var/lib/danube",     ""),
        ];

        for (input, expected_root, expected_prefix) in cases {
            let res = crate::cloud_store::split_fs_root(input).expect("expected Ok");
            assert_eq!(res.0, expected_root);
            assert_eq!(res.1, expected_prefix);
        }
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
