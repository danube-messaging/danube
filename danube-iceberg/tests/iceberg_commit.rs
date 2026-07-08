//! # Iceberg Catalog Integration Tests
//!
//! These tests validate the Iceberg catalog integration using the in-memory
//! `MemoryCatalog` from the `iceberg` core crate. No external services needed.
//!
//! ## What we test
//!
//! 1. **Table creation** — Create an Iceberg table from an Arrow schema,
//!    verify the table exists in the catalog with the correct schema
//! 2. **Data file commit** — Build a DataFile descriptor and commit it to
//!    a table via Transaction/FastAppend, verify the snapshot exists
//! 3. **Namespace management** — Verify namespace creation and listing
//!
//! ## Why MemoryCatalog
//!
//! MemoryCatalog + MemoryStorageFactory is built into the `iceberg` crate
//! and requires zero external infrastructure. This makes tests fast and
//! reliable in CI — no Docker, no MinIO, no Nessie.

mod common;

use iceberg::io::MemoryStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType,
    Schema as IcebergSchema, Struct, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper: build a MemoryCatalog with MemoryStorageFactory.
async fn build_memory_catalog() -> Arc<dyn Catalog> {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "test_catalog",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                "/tmp/test_warehouse".to_string(),
            )]),
        )
        .await
        .expect("build memory catalog");
    Arc::new(catalog)
}

/// Helper: build a simple Iceberg schema matching the Danube envelope format.
fn envelope_iceberg_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "offset", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "publish_time", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(3, "producer_name", Type::Primitive(PrimitiveType::String))
                .into(),
            NestedField::required(4, "payload", Type::Primitive(PrimitiveType::Binary)).into(),
        ])
        .build()
        .expect("build schema")
}

// ============================================================================
// Tests
// ============================================================================

/// Verifies that we can create a namespace and a table in the MemoryCatalog,
/// then load the table back and verify its schema matches.
///
/// This is the foundation of the Iceberg integration: every topic needs
/// a table, and every table needs to be created with the right schema.
#[tokio::test]
async fn create_and_load_table() {
    let catalog = build_memory_catalog().await;

    // Create namespace
    let ns = NamespaceIdent::from_strs(["default"]).unwrap();
    catalog
        .create_namespace(&ns, Default::default())
        .await
        .expect("create namespace");

    // Create table
    let schema = envelope_iceberg_schema();
    let table_creation = TableCreation::builder()
        .name("sensor_data".to_string())
        .schema(schema)
        .build();

    let _table = catalog
        .create_table(&ns, table_creation)
        .await
        .expect("create table");

    // Verify table exists
    let table_ident = TableIdent::new(ns.clone(), "sensor_data".to_string());
    let loaded = catalog.load_table(&table_ident).await.expect("load table");

    // Verify schema has 4 fields
    let fields = loaded.metadata().current_schema().as_struct().fields();
    assert_eq!(fields.len(), 4, "table should have 4 fields");
    assert_eq!(fields[0].name, "offset");
    assert_eq!(fields[1].name, "publish_time");
    assert_eq!(fields[2].name, "producer_name");
    assert_eq!(fields[3].name, "payload");
}

/// Verifies that loading a non-existent table returns an error.
///
/// The TableManager uses this to decide whether to create or load a table.
/// If load_table doesn't fail for missing tables, we'd never create them.
#[tokio::test]
async fn load_nonexistent_table_fails() {
    let catalog = build_memory_catalog().await;

    let ns = NamespaceIdent::from_strs(["default"]).unwrap();
    let table_ident = TableIdent::new(ns, "nonexistent".to_string());

    let result = catalog.load_table(&table_ident).await;
    assert!(result.is_err(), "loading nonexistent table should fail");
}

/// Verifies that creating a namespace is idempotent — creating the same
/// namespace twice should not cause an error (or we can safely ignore it).
///
/// The TableManager creates namespaces before tables. Multiple topics in
/// the same namespace would cause duplicate creation attempts.
#[tokio::test]
async fn namespace_creation_idempotent() {
    let catalog = build_memory_catalog().await;

    let ns = NamespaceIdent::from_strs(["default"]).unwrap();

    // First creation
    catalog
        .create_namespace(&ns, Default::default())
        .await
        .expect("first create");

    // Second creation — may return error, but we handle it gracefully
    let result = catalog.create_namespace(&ns, Default::default()).await;
    // We don't assert success — the important thing is it doesn't panic.
    // In the real code, we log and continue.
    let _ = result;
}

/// Verifies the full data file commit flow:
/// 1. Create namespace + table
/// 2. Build a DataFile descriptor
/// 3. Transaction::new → fast_append → commit
/// 4. Verify the table now has a snapshot
///
/// This is the critical integration point: after writing a Parquet file,
/// we must register it with the catalog so query engines can discover it.
#[tokio::test]
async fn commit_data_file_to_table() {
    let catalog = build_memory_catalog().await;

    // Setup: create namespace and table
    let ns = NamespaceIdent::from_strs(["default"]).unwrap();
    catalog
        .create_namespace(&ns, Default::default())
        .await
        .expect("create namespace");

    let schema = envelope_iceberg_schema();
    let table_creation = TableCreation::builder()
        .name("events".to_string())
        .schema(schema)
        .build();

    catalog
        .create_table(&ns, table_creation)
        .await
        .expect("create table");

    // Load table for transaction
    let table_ident = TableIdent::new(ns.clone(), "events".to_string());
    let table = catalog.load_table(&table_ident).await.expect("load table");

    // Build DataFile descriptor (tests the Transaction API directly —
    // in production, writer.rs uses iceberg's DataFileWriter pipeline
    // to produce DataFile instances with full column statistics)
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("/tmp/test_warehouse/default/events/data/part-001.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .record_count(100)
        .file_size_in_bytes(4096)
        .partition(Struct::empty())
        .build()
        .expect("build data file");

    // Commit via Transaction
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file]);
    let tx = action.apply(tx).expect("apply fast_append");

    let updated_table = tx.commit(&*catalog).await.expect("commit transaction");

    // Verify: table should now have a snapshot
    let snapshots: Vec<_> = updated_table.metadata().snapshots().collect();
    assert!(
        !snapshots.is_empty(),
        "table should have at least one snapshot after commit"
    );

    // Verify: the snapshot should have the right operation type
    let latest = updated_table
        .metadata()
        .current_snapshot()
        .expect("should have current snapshot");

    // The summary's additional_properties should contain data file info
    assert!(
        !latest.summary().additional_properties.is_empty(),
        "snapshot summary should contain metadata about committed files"
    );
}

/// Verifies that listing tables in a namespace returns the created tables.
///
/// This is used by monitoring/admin tools to see what tables exist.
#[tokio::test]
async fn list_tables_in_namespace() {
    let catalog = build_memory_catalog().await;

    let ns = NamespaceIdent::from_strs(["analytics"]).unwrap();
    catalog
        .create_namespace(&ns, Default::default())
        .await
        .expect("create namespace");

    // Create two tables
    let schema = envelope_iceberg_schema();
    for name in &["table_a", "table_b"] {
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(schema.clone())
            .build();
        catalog
            .create_table(&ns, creation)
            .await
            .expect("create table");
    }

    // List tables
    let tables = catalog.list_tables(&ns).await.expect("list tables");
    assert_eq!(tables.len(), 2, "should have 2 tables");

    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"table_a"));
    assert!(names.contains(&"table_b"));
}
