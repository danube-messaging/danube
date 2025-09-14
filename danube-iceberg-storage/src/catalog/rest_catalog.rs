use crate::errors::{IcebergStorageError, Result};
use async_trait::async_trait;

// Import the types from the parent catalog module
use super::{DataFile, IcebergCatalog, PlannedFile, TableMetadata, TableSchema};

// Official Iceberg crates
use iceberg::spec::{
    NestedField, PrimitiveType, Schema as IcebergSchema, StructType, Type as IcebergType,
};
use iceberg::table::Table;
use iceberg::{Catalog as IcebergCatalogTrait, Namespace, TableIdent};
use iceberg_catalog_rest as rest;

/// REST catalog wrapper implementation using the official Iceberg REST catalog
#[derive(Debug)]
pub struct RestCatalog {
    inner: rest::RestCatalog,
    warehouse: String,
}

impl RestCatalog {
    pub async fn new(uri: &str, warehouse: &str) -> Result<Self> {
        let inner = rest::RestCatalog::new(uri, Some(warehouse.to_string()), None)
            .await
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Failed to create REST catalog: {}", e))
            })?;
        Ok(Self {
            inner,
            warehouse: warehouse.to_string(),
        })
    }

    fn to_namespace(ns: &str) -> Namespace {
        // Iceberg namespaces can be multi-part; we treat our ns as single-part
        Namespace::from(vec![ns.to_string()])
    }

    fn to_ident(ns: &str, table: &str) -> Result<TableIdent> {
        TableIdent::from_parts(vec![ns.to_string(), table.to_string()]).map_err(|e| {
            IcebergStorageError::Catalog(format!("Invalid table ident: {}.{} ({})", ns, table, e))
        })
    }

    fn convert_schema(schema: &TableSchema) -> Result<IcebergSchema> {
        let mut fields = Vec::new();
        for f in &schema.fields {
            let ty = match f.field_type.as_str() {
                "long" => IcebergType::Primitive(PrimitiveType::Long),
                "string" => IcebergType::Primitive(PrimitiveType::String),
                "binary" => IcebergType::Primitive(PrimitiveType::Binary),
                "timestamp" => IcebergType::Primitive(PrimitiveType::Timestamp),
                other => {
                    return Err(IcebergStorageError::Catalog(format!(
                        "Unsupported field type in schema: {}",
                        other
                    ))
                    .into())
                }
            };
            let nf = NestedField::optional(f.id as i32, f.name.clone(), ty, None);
            // Respect required flag by toggling optional/required
            let nf = if f.required {
                NestedField::required(f.id as i32, f.name.clone(), nf.field_type().clone(), None)
            } else {
                nf
            };
            fields.push(nf);
        }
        let struct_ty = StructType::new(fields);
        let schema = IcebergSchema::builder()
            .with_fields(struct_ty)
            .build()
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Failed to build Iceberg schema: {}", e))
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
impl IcebergCatalog for RestCatalog {
    async fn create_table(
        &self,
        namespace: &str,
        table_name: &str,
        schema: &TableSchema,
        location: &str,
    ) -> Result<TableMetadata> {
        // Build Iceberg schema
        let iceberg_schema = Self::convert_schema(schema)?;
        let ident = Self::to_ident(namespace, table_name)?;

        let tbl = self
            .inner
            .create_table(&ident, iceberg_schema, Some(location.to_string()), None)
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("Create table failed: {}", e)))?;

        Ok(Self::minimal_metadata_from_table(&tbl))
    }

    async fn load_table(&self, namespace: &str, table_name: &str) -> Result<TableMetadata> {
        let ident = Self::to_ident(namespace, table_name)?;
        let table = self
            .inner
            .load_table(&ident)
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("Load table failed: {}", e)))?;
        Ok(Self::minimal_metadata_from_table(&table))
    }

    async fn update_table(
        &self,
        namespace: &str,
        table_name: &str,
        _current_metadata: &TableMetadata,
        _new_metadata: &TableMetadata,
    ) -> Result<TableMetadata> {
        // Updating full metadata via REST is beyond Stage 1 scope.
        // Load and return current minimal metadata.
        self.load_table(namespace, table_name).await
    }

    async fn table_exists(&self, namespace: &str, table_name: &str) -> Result<bool> {
        let ident = Self::to_ident(namespace, table_name)?;
        let exists = self.inner.table_exists(&ident).await.map_err(|e| {
            IcebergStorageError::Catalog(format!("table_exists check failed: {}", e))
        })?;
        Ok(exists)
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let ns = Self::to_namespace(namespace);
        let idents = self
            .inner
            .list_tables(&ns)
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("List tables failed: {}", e)))?;
        Ok(idents.into_iter().map(|ti| ti.name().to_string()).collect())
    }

    async fn drop_table(&self, namespace: &str, table_name: &str) -> Result<()> {
        let ident = Self::to_ident(namespace, table_name)?;
        self.inner
            .drop_table(&ident)
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("Drop table failed: {}", e)))?;
        Ok(())
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let ns = Self::to_namespace(namespace);
        self.inner
            .create_namespace(&ns, None)
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("Create namespace failed: {}", e)))?;
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let nss =
            self.inner.list_namespaces(None).await.map_err(|e| {
                IcebergStorageError::Catalog(format!("List namespaces failed: {}", e))
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
        Err(IcebergStorageError::Catalog(
            "commit_add_files is handled via Iceberg writer in later stages".to_string(),
        ))
    }

    async fn plan_scan(
        &self,
        _namespace: &str,
        _table_name: &str,
        _from_snapshot_id: Option<i64>,
        _to_snapshot_id: Option<i64>,
    ) -> Result<Vec<PlannedFile>> {
        Err(IcebergStorageError::Catalog(
            "plan_scan is handled via Iceberg scan API in later stages".to_string(),
        ))
    }
}
