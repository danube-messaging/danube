use crate::errors::{IcebergStorageError, Result};
use async_trait::async_trait;
use aws_sdk_glue::types::{DatabaseInput, SerDeInfo, StorageDescriptor, TableInput};
use aws_sdk_glue::Client;
use uuid::Uuid;

// Import the types from the parent catalog module
use super::{create_danube_schema, IcebergCatalog, TableMetadata, TableSchema};

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
