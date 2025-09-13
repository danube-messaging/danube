use crate::config::CatalogConfig;
use crate::errors::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub mod glue_catalog;
pub mod rest_catalog;

pub use glue_catalog::GlueCatalog;
pub use rest_catalog::RestCatalog;

/// Iceberg table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub last_sequence_number: i64,
    pub last_updated_ms: i64,
    pub schema: TableSchema,
    pub current_schema_id: i32,
    pub snapshots: Vec<Snapshot>,
    pub current_snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub schema_id: i32,
    pub fields: Vec<NestedField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
    pub schema_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    pub content: String,
    pub file_path: String,
    pub file_format: String,
    pub partition: HashMap<String, String>,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub column_sizes: HashMap<String, i64>,
    pub value_counts: HashMap<String, i64>,
    pub null_value_counts: HashMap<String, i64>,
    pub nan_value_counts: HashMap<String, i64>,
    pub lower_bounds: HashMap<String, String>,
    pub upper_bounds: HashMap<String, String>,
    pub key_metadata: Option<String>,
    pub split_offsets: Vec<i64>,
    pub equality_ids: Vec<i32>,
    pub sort_order_id: Option<i32>,
}

/// Planned file returned by scan planning (status is typically "ADDED" or "DELETED")
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedFile {
    pub file_path: String,
    pub status: String,
}

/// Trait for Iceberg catalog operations
#[async_trait]
pub trait IcebergCatalog: Send + Sync + std::fmt::Debug {
    async fn create_table(
        &self,
        namespace: &str,
        table_name: &str,
        schema: &TableSchema,
        location: &str,
    ) -> Result<TableMetadata>;

    async fn load_table(&self, namespace: &str, table_name: &str) -> Result<TableMetadata>;

    async fn update_table(
        &self,
        namespace: &str,
        table_name: &str,
        current_metadata: &TableMetadata,
        new_metadata: &TableMetadata,
    ) -> Result<TableMetadata>;

    async fn table_exists(&self, namespace: &str, table_name: &str) -> Result<bool>;

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>>;

    async fn drop_table(&self, namespace: &str, table_name: &str) -> Result<()>;

    async fn create_namespace(&self, namespace: &str) -> Result<()>;

    async fn list_namespaces(&self) -> Result<Vec<String>>;

    /// Commit new data files to the table using Iceberg REST commit protocol
    async fn commit_add_files(
        &self,
        namespace: &str,
        table_name: &str,
        current_metadata: &TableMetadata,
        data_files: Vec<DataFile>,
    ) -> Result<TableMetadata>;

    /// Plan a scan between two snapshots and return planned data files with status
    async fn plan_scan(
        &self,
        namespace: &str,
        table_name: &str,
        from_snapshot_id: Option<i64>,
        to_snapshot_id: Option<i64>,
    ) -> Result<Vec<PlannedFile>>;
}

/// Create catalog based on configuration
pub async fn create_catalog(
    config: &CatalogConfig,
    warehouse: &str,
) -> Result<Arc<dyn IcebergCatalog>> {
    match config {
        CatalogConfig::Rest { uri, .. } => {
            let catalog = RestCatalog::new(uri, warehouse).await?;
            Ok(Arc::new(catalog))
        }
        CatalogConfig::Glue {
            region, database, ..
        } => {
            let catalog = GlueCatalog::new(region, database, warehouse).await?;
            Ok(Arc::new(catalog))
        }
    }
}

/// Create the standard Danube message schema
pub fn create_danube_schema() -> TableSchema {
    TableSchema {
        schema_id: 0,
        fields: vec![
            NestedField {
                id: 1,
                name: "request_id".to_string(),
                required: true,
                field_type: "long".to_string(),
            },
            NestedField {
                id: 2,
                name: "producer_id".to_string(),
                required: true,
                field_type: "long".to_string(),
            },
            NestedField {
                id: 3,
                name: "topic_name".to_string(),
                required: true,
                field_type: "string".to_string(),
            },
            NestedField {
                id: 4,
                name: "broker_addr".to_string(),
                required: true,
                field_type: "string".to_string(),
            },
            NestedField {
                id: 5,
                name: "segment_id".to_string(),
                required: true,
                field_type: "long".to_string(),
            },
            NestedField {
                id: 6,
                name: "segment_offset".to_string(),
                required: true,
                field_type: "long".to_string(),
            },
            NestedField {
                id: 7,
                name: "payload".to_string(),
                required: true,
                field_type: "binary".to_string(),
            },
            NestedField {
                id: 8,
                name: "publish_time".to_string(),
                required: true,
                field_type: "timestamp".to_string(),
            },
            NestedField {
                id: 9,
                name: "producer_name".to_string(),
                required: true,
                field_type: "string".to_string(),
            },
            NestedField {
                id: 10,
                name: "subscription_name".to_string(),
                required: false,
                field_type: "string".to_string(),
            },
        ],
    }
}
