use crate::errors::{IcebergStorageError, Result};
use async_trait::async_trait;
use url::Url;

// Import the types from the parent catalog module
use super::{DataFile, IcebergCatalog, TableMetadata, TableSchema};

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

    fn commit_url(&self, namespace: &str, table_name: &str) -> Result<Url> {
        let mut url = self.table_url(namespace, table_name)?;
        url.path_segments_mut()
            .map_err(|_| IcebergStorageError::Config("Invalid base URL".to_string()))?
            .push("commit");
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

    async fn commit_add_files(
        &self,
        namespace: &str,
        table_name: &str,
        current_metadata: &TableMetadata,
        data_files: Vec<DataFile>,
    ) -> Result<TableMetadata> {
        // Build commit request per Iceberg REST spec
        // Minimal requirements: assert table uuid and optionally current snapshot id
        let mut requirements = vec![serde_json::json!({
            "type": "assert-table-uuid",
            "uuid": current_metadata.table_uuid
        })];

        if let Some(sid) = current_metadata.current_snapshot_id {
            requirements.push(serde_json::json!({
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": sid
            }));
        }

        let add_op = serde_json::json!({
            "type": "add",
            "data-files": data_files
        });

        let body = serde_json::json!({
            "requirements": requirements,
            "operations": [ add_op ]
        });

        let url = self.commit_url(namespace, table_name)?;
        let response = self
            .client
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(IcebergStorageError::Catalog(format!(
                "Commit add files failed with status: {}",
                response.status()
            )));
        }

        // Response should include updated table metadata or ref
        let metadata: TableMetadata = response.json().await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Failed to parse response: {}", e))
        })?;

        Ok(metadata)
    }
}
