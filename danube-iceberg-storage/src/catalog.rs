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

// Official Iceberg crates for Memory catalog support
use iceberg::io::FileIOBuilder;
use iceberg::memory::MemoryCatalog as IcebergMemoryCatalog;
use iceberg::spec::{Schema as IcebergSchema, Type as IcebergType};
use iceberg::table::Table;
use iceberg::{Catalog as IcebergCatalogTrait, Namespace, TableIdent};

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
        CatalogConfig::Memory {} => {
            let catalog = MemoryCatalogImpl::new().await?;
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

/// Minimal wrapper around Iceberg's in-memory catalog to satisfy our IcebergCatalog trait
#[derive(Debug)]
struct MemoryCatalogImpl {
    inner: IcebergMemoryCatalog,
}

impl MemoryCatalogImpl {
    async fn new() -> Result<Self> {
        let io = FileIOBuilder::new("memory").build().map_err(|e| {
            crate::errors::IcebergStorageError::Catalog(format!(
                "Failed to build memory FileIO: {}",
                e
            ))
        })?;
        Ok(Self {
            inner: IcebergMemoryCatalog::new(io, None),
        })
    }

    fn to_ident(ns: &str, table: &str) -> Result<TableIdent> {
        TableIdent::from_parts(vec![ns.to_string(), table.to_string()]).map_err(|e| {
            crate::errors::IcebergStorageError::Catalog(format!(
                "Invalid table ident: {}.{} ({})",
                ns, table, e
            ))
            .into()
        })
    }

    fn convert_schema(schema: &TableSchema) -> Result<IcebergSchema> {
        use iceberg::spec::{NestedField, PrimitiveType, StructType};
        let mut fields = Vec::new();
        for f in &schema.fields {
            let ty = match f.field_type.as_str() {
                "long" => IcebergType::Primitive(PrimitiveType::Long),
                "string" => IcebergType::Primitive(PrimitiveType::String),
                "binary" => IcebergType::Primitive(PrimitiveType::Binary),
                "timestamp" => IcebergType::Primitive(PrimitiveType::Timestamp),
                other => {
                    return Err(crate::errors::IcebergStorageError::Catalog(format!(
                        "Unsupported field type in schema: {}",
                        other
                    ))
                    .into())
                }
            };
            let nf = if f.required {
                NestedField::required(f.id as i32, f.name.clone(), ty, None)
            } else {
                NestedField::optional(f.id as i32, f.name.clone(), ty, None)
            };
            fields.push(nf);
        }
        let struct_ty = StructType::new(fields);
        let schema = IcebergSchema::builder()
            .with_fields(struct_ty)
            .build()
            .map_err(|e| {
                crate::errors::IcebergStorageError::Catalog(format!(
                    "Failed to build Iceberg schema: {}",
                    e
                ))
            })?;
        Ok(schema)
    }

    fn minimal_metadata_from_table(table: &Table) -> TableMetadata {
        let current_snapshot_id = table.current_snapshot().map(|s| s.snapshot_id());
        TableMetadata {
            format_version: 2,
            table_uuid: table
                .metadata()
                .table_uuid()
                .unwrap_or_default()
                .to_string(),
            location: table.metadata().location().to_string(),
            last_sequence_number: 0,
            last_updated_ms: 0,
            schema: TableSchema {
                schema_id: table.metadata().current_schema_id(),
                fields: vec![],
            },
            current_schema_id: table.metadata().current_schema_id(),
            snapshots: vec![],
            current_snapshot_id: current_snapshot_id.map(|id| id as i64),
        }
    }
}

#[async_trait]
impl IcebergCatalog for MemoryCatalogImpl {
    async fn create_table(
        &self,
        namespace: &str,
        table_name: &str,
        schema: &TableSchema,
        location: &str,
    ) -> Result<TableMetadata> {
        let iceberg_schema = Self::convert_schema(schema)?;
        let ident = Self::to_ident(namespace, table_name)?;
        let tbl = self
            .inner
            .create_table(&ident, iceberg_schema, Some(location.to_string()), None)
            .await
            .map_err(|e| {
                crate::errors::IcebergStorageError::Catalog(format!(
                    "Memory create table failed: {}",
                    e
                ))
            })?;
        Ok(Self::minimal_metadata_from_table(&tbl))
    }

    async fn load_table(&self, namespace: &str, table_name: &str) -> Result<TableMetadata> {
        let ident = Self::to_ident(namespace, table_name)?;
        let table = self.inner.load_table(&ident).await.map_err(|e| {
            crate::errors::IcebergStorageError::Catalog(format!("Memory load table failed: {}", e))
        })?;
        Ok(Self::minimal_metadata_from_table(&table))
    }

    async fn update_table(
        &self,
        namespace: &str,
        table_name: &str,
        _current_metadata: &TableMetadata,
        _new_metadata: &TableMetadata,
    ) -> Result<TableMetadata> {
        self.load_table(namespace, table_name).await
    }

    async fn table_exists(&self, namespace: &str, table_name: &str) -> Result<bool> {
        let ident = Self::to_ident(namespace, table_name)?;
        let exists = self.inner.table_exists(&ident).await.map_err(|e| {
            crate::errors::IcebergStorageError::Catalog(format!(
                "Memory table_exists failed: {}",
                e
            ))
        })?;
        Ok(exists)
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let idents = self
            .inner
            .list_tables(&Namespace::from(vec![namespace.to_string()]))
            .await
            .map_err(|e| {
                crate::errors::IcebergStorageError::Catalog(format!(
                    "Memory list tables failed: {}",
                    e
                ))
            })?;
        Ok(idents.into_iter().map(|ti| ti.name().to_string()).collect())
    }

    async fn drop_table(&self, namespace: &str, table_name: &str) -> Result<()> {
        let ident = Self::to_ident(namespace, table_name)?;
        self.inner.drop_table(&ident).await.map_err(|e| {
            crate::errors::IcebergStorageError::Catalog(format!("Memory drop table failed: {}", e))
        })?;
        Ok(())
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        self.inner
            .create_namespace(&Namespace::from(vec![namespace.to_string()]), None)
            .await
            .map_err(|e| {
                crate::errors::IcebergStorageError::Catalog(format!(
                    "Memory create namespace failed: {}",
                    e
                ))
            })?;
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let nss = self.inner.list_namespaces(None).await.map_err(|e| {
            crate::errors::IcebergStorageError::Catalog(format!(
                "Memory list namespaces failed: {}",
                e
            ))
        })?;
        Ok(nss.into_iter().map(|ns| ns.to_string()).collect())
    }

    async fn commit_add_files(
        &self,
        _namespace: &str,
        _table_name: &str,
        _current_metadata: &TableMetadata,
        _data_files: Vec<DataFile>,
    ) -> Result<TableMetadata> {
        Err(crate::errors::IcebergStorageError::Catalog(
            "commit_add_files is handled via Iceberg writer in later stages".to_string(),
        )
        .into())
    }

    async fn plan_scan(
        &self,
        _namespace: &str,
        _table_name: &str,
        _from_snapshot_id: Option<i64>,
        _to_snapshot_id: Option<i64>,
    ) -> Result<Vec<PlannedFile>> {
        Err(crate::errors::IcebergStorageError::Catalog(
            "plan_scan is handled via Iceberg scan API in later stages".to_string(),
        )
        .into())
    }
}
