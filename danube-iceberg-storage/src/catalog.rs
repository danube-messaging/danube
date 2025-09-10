use crate::config::CatalogConfig;
use crate::errors::{IcebergStorageError, Result};
use async_trait::async_trait;
use aws_sdk_glue::types::{DatabaseInput, SerDeInfo, StorageDescriptor, TableInput};
use aws_sdk_glue::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

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
}

/// REST catalog implementation
#[derive(Debug)]
pub struct RestCatalog {
    client: reqwest::Client,
    base_url: Url,
    warehouse: String,
}

impl RestCatalog {
    pub async fn new(uri: &str, warehouse: &str) -> Result<Self> {
        let base_url = Url::parse(uri)
            .map_err(|e| IcebergStorageError::Config(format!("Invalid REST catalog URI: {}", e)))?;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                IcebergStorageError::Config(format!("Failed to create HTTP client: {}", e))
            })?;

        Ok(Self {
            client,
            base_url,
            warehouse: warehouse.to_string(),
        })
    }

    fn table_url(&self, namespace: &str, table_name: &str) -> Result<Url> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| IcebergStorageError::Config("Invalid base URL".to_string()))?
            .push("v1")
            .push("namespaces")
            .push(namespace)
            .push("tables")
            .push(table_name);
        Ok(url)
    }
}

#[async_trait]
impl IcebergCatalog for RestCatalog {
    async fn create_table(
        &self,
        namespace: &str,
        table_name: &str,
        schema: &TableSchema,
        location: &str,
    ) -> Result<TableMetadata> {
        let url = self.table_url(namespace, table_name)?;

        let create_request = serde_json::json!({
            "name": table_name,
            "location": location,
            "schema": schema
        });

        let response = self
            .client
            .post(url)
            .json(&create_request)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(IcebergStorageError::Catalog(format!(
                "Create table failed with status: {}",
                response.status()
            )));
        }

        let metadata: TableMetadata = response.json().await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Failed to parse response: {}", e))
        })?;

        Ok(metadata)
    }

    async fn load_table(&self, namespace: &str, table_name: &str) -> Result<TableMetadata> {
        let url = self.table_url(namespace, table_name)?;

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(IcebergStorageError::Catalog(format!(
                "Load table failed with status: {}",
                response.status()
            )));
        }

        let metadata: TableMetadata = response.json().await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Failed to parse response: {}", e))
        })?;

        Ok(metadata)
    }

    async fn update_table(
        &self,
        namespace: &str,
        table_name: &str,
        _current_metadata: &TableMetadata,
        new_metadata: &TableMetadata,
    ) -> Result<TableMetadata> {
        let url = self.table_url(namespace, table_name)?;

        let response = self
            .client
            .post(url)
            .json(new_metadata)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(IcebergStorageError::Catalog(format!(
                "Update table failed with status: {}",
                response.status()
            )));
        }

        let metadata: TableMetadata = response.json().await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Failed to parse response: {}", e))
        })?;

        Ok(metadata)
    }

    async fn table_exists(&self, namespace: &str, table_name: &str) -> Result<bool> {
        let url = self.table_url(namespace, table_name)?;

        let response = self
            .client
            .head(url)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        Ok(response.status().is_success())
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| IcebergStorageError::Config("Invalid base URL".to_string()))?
            .push("v1")
            .push("namespaces")
            .push(namespace)
            .push("tables");

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(IcebergStorageError::Catalog(format!(
                "List tables failed with status: {}",
                response.status()
            )));
        }

        let tables: serde_json::Value = response.json().await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Failed to parse response: {}", e))
        })?;

        let table_names = tables["identifiers"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v["name"].as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();

        Ok(table_names)
    }

    async fn drop_table(&self, namespace: &str, table_name: &str) -> Result<()> {
        let url = self.table_url(namespace, table_name)?;

        let response = self
            .client
            .delete(url)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(IcebergStorageError::Catalog(format!(
                "Drop table failed with status: {}",
                response.status()
            )));
        }

        Ok(())
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| IcebergStorageError::Config("Invalid base URL".to_string()))?
            .push("v1")
            .push("namespaces");

        let create_request = serde_json::json!({
            "namespace": [namespace],
            "properties": {}
        });

        let response = self
            .client
            .post(url)
            .json(&create_request)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(IcebergStorageError::Catalog(format!(
                "Create namespace failed with status: {}",
                response.status()
            )));
        }

        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .map_err(|_| IcebergStorageError::Config("Invalid base URL".to_string()))?
            .push("v1")
            .push("namespaces");

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(IcebergStorageError::Catalog(format!(
                "List namespaces failed with status: {}",
                response.status()
            )));
        }

        let namespaces: serde_json::Value = response.json().await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Failed to parse response: {}", e))
        })?;

        let namespace_names = namespaces["namespaces"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v[0].as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();

        Ok(namespace_names)
    }
}

/// AWS Glue catalog implementation
#[derive(Debug)]
pub struct GlueCatalog {
    client: Client,
    database: String,
    warehouse: String,
}

impl GlueCatalog {
    pub async fn new(region: &str, database: &str, warehouse: &str) -> Result<Self> {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;

        let client = Client::new(&config);

        Ok(Self {
            client,
            database: database.to_string(),
            warehouse: warehouse.to_string(),
        })
    }
}

#[async_trait]
impl IcebergCatalog for GlueCatalog {
    async fn create_table(
        &self,
        _namespace: &str,
        table_name: &str,
        schema: &TableSchema,
        location: &str,
    ) -> Result<TableMetadata> {
        let serde_info = SerDeInfo::builder()
            .serialization_library("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
            .build();

        let storage_descriptor = StorageDescriptor::builder()
            .location(location)
            .input_format("org.apache.hadoop.mapred.TextInputFormat")
            .output_format("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
            .serde_info(serde_info)
            .build();

        let table_input = TableInput::builder()
            .name(table_name)
            .storage_descriptor(storage_descriptor)
            .build();

        self.client
            .create_table()
            .database_name(&self.database)
            .table_input(table_input.expect("Failed to build TableInput"))
            .send()
            .await
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Glue create table failed: {}", e))
            })?;

        // Return a basic metadata structure
        Ok(TableMetadata {
            format_version: 2,
            table_uuid: Uuid::new_v4().to_string(),
            location: location.to_string(),
            last_sequence_number: 0,
            last_updated_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            schema: schema.clone(),
            current_schema_id: schema.schema_id,
            snapshots: vec![],
            current_snapshot_id: None,
        })
    }

    async fn load_table(&self, _namespace: &str, table_name: &str) -> Result<TableMetadata> {
        let response = self
            .client
            .get_table()
            .database_name(&self.database)
            .name(table_name)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("Glue get table failed: {}", e)))?;

        let table = response.table().ok_or_else(|| {
            IcebergStorageError::Catalog("Table not found in Glue response".to_string())
        })?;

        // Convert Glue table to Iceberg metadata
        let location = table
            .storage_descriptor()
            .and_then(|sd| sd.location())
            .unwrap_or("")
            .to_string();

        Ok(TableMetadata {
            format_version: 2,
            table_uuid: Uuid::new_v4().to_string(),
            location,
            last_sequence_number: 0,
            last_updated_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            schema: create_danube_schema(),
            current_schema_id: 0,
            snapshots: vec![],
            current_snapshot_id: None,
        })
    }

    async fn update_table(
        &self,
        _namespace: &str,
        table_name: &str,
        _current_metadata: &TableMetadata,
        new_metadata: &TableMetadata,
    ) -> Result<TableMetadata> {
        let serde_info = SerDeInfo::builder()
            .serialization_library("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
            .build();

        let storage_descriptor = StorageDescriptor::builder()
            .location(&new_metadata.location)
            .input_format("org.apache.hadoop.mapred.TextInputFormat")
            .output_format("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
            .serde_info(serde_info)
            .build();

        let table_input = TableInput::builder()
            .name(table_name)
            .storage_descriptor(storage_descriptor)
            .build();

        self.client
            .update_table()
            .database_name(&self.database)
            .table_input(table_input.expect("Failed to build TableInput"))
            .send()
            .await
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Glue update table failed: {}", e))
            })?;

        Ok(new_metadata.clone())
    }

    async fn table_exists(&self, _namespace: &str, table_name: &str) -> Result<bool> {
        match self
            .client
            .get_table()
            .database_name(&self.database)
            .name(table_name)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn list_tables(&self, _namespace: &str) -> Result<Vec<String>> {
        let response = self
            .client
            .get_tables()
            .database_name(&self.database)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("Glue list tables failed: {}", e)))?;

        let table_names = response
            .table_list()
            .iter()
            .filter_map(|table| Some(table.name()))
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        Ok(table_names)
    }

    async fn drop_table(&self, _namespace: &str, table_name: &str) -> Result<()> {
        self.client
            .delete_table()
            .database_name(&self.database)
            .name(table_name)
            .send()
            .await
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Glue delete table failed: {}", e))
            })?;

        Ok(())
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let database_input = DatabaseInput::builder()
            .name(namespace)
            .description("Danube messaging namespace")
            .build();

        self.client
            .create_database()
            .database_input(database_input.expect("Failed to build DatabaseInput"))
            .send()
            .await
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Glue create database failed: {}", e))
            })?;

        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let response = self.client.get_databases().send().await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Glue list databases failed: {}", e))
        })?;

        let database_names = response
            .database_list()
            .iter()
            .filter_map(|db| Some(db.name()))
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        Ok(database_names)
    }
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
