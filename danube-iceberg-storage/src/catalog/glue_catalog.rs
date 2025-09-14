use crate::errors::{IcebergStorageError, Result};
use async_trait::async_trait;

// Import the types from the parent catalog module
use super::IcebergCatalog;

// Official Iceberg crates
use iceberg::spec::Schema as IcebergSchema;
use iceberg::transaction::Transaction;
use iceberg::NamespaceIdent;
use iceberg::{table::Table, Catalog as IcebergCatalogTrait, TableIdent};
use iceberg_catalog_glue as glue;
use std::collections::HashMap;

/// AWS Glue catalog wrapper using official Iceberg Glue catalog
#[derive(Debug)]
pub struct GlueCatalog {
    inner: glue::GlueCatalog,
    warehouse: String,
}

impl GlueCatalog {
    pub async fn new(_region: &str, _database: &str, warehouse: &str) -> Result<Self> {
        // Build config using builder per docs; region/account are discovered via AWS SDK env/config
        let cfg = glue::GlueCatalogConfig::builder()
            .warehouse(warehouse.to_string())
            .build()
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Failed to build Glue catalog config: {}", e))
            })?;
        let inner = glue::GlueCatalog::new(cfg).await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Failed to create Glue catalog: {}", e))
        })?;
        Ok(Self {
            inner,
            warehouse: warehouse.to_string(),
        })
    }

    fn to_namespace(ns: &str) -> NamespaceIdent {
        NamespaceIdent::from_strs(vec![ns.to_string()]).expect("namespace ident")
    }

    fn to_ident(ns: &str, table: &str) -> Result<TableIdent> {
        TableIdent::from_strs(vec![ns.to_string(), table.to_string()]).map_err(|e| {
            IcebergStorageError::Catalog(format!("Invalid table ident: {}.{} ({})", ns, table, e))
        })
    }
}

#[async_trait]
impl IcebergCatalog for GlueCatalog {
    async fn create_table(
        &self,
        namespace: &str,
        table_name: &str,
        schema: &IcebergSchema,
        location: &str,
    ) -> Result<()> {
        use iceberg::TableCreation;
        let ns = Self::to_namespace(namespace);
        let creation = TableCreation::builder()
            .name(table_name.to_string())
            .schema(schema.clone())
            .location(location.to_string())
            .build()
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Glue create table failed: {}", e))
            })?;
        self.inner.create_table(&ns, creation).await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Glue create table failed: {}", e))
        })?;
        Ok(())
    }

    async fn table_exists(&self, namespace: &str, table_name: &str) -> Result<bool> {
        let ident = Self::to_ident(namespace, table_name)?;
        let exists = self.inner.table_exists(&ident).await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Glue table_exists failed: {}", e))
        })?;
        Ok(exists)
    }

    async fn list_tables(&self, namespace: &str) -> Result<Vec<String>> {
        let ns = Self::to_namespace(namespace);
        let idents =
            self.inner.list_tables(&ns).await.map_err(|e| {
                IcebergStorageError::Catalog(format!("Glue list tables failed: {}", e))
            })?;
        Ok(idents.into_iter().map(|ti| ti.name().to_string()).collect())
    }

    async fn drop_table(&self, namespace: &str, table_name: &str) -> Result<()> {
        let ident = Self::to_ident(namespace, table_name)?;
        self.inner
            .drop_table(&ident)
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("Glue drop table failed: {}", e)))?;
        Ok(())
    }

    async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let ns = Self::to_namespace(namespace);
        self.inner
            .create_namespace(&ns, HashMap::new())
            .await
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Glue create namespace failed: {}", e))
            })?;
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        let ns = self.inner.list_namespaces(None).await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Glue list namespaces failed: {}", e))
        })?;
        Ok(ns.into_iter().map(|n| n.to_string()).collect())
    }

    async fn load_table_handle(&self, namespace: &str, table_name: &str) -> Result<Table> {
        let ident = Self::to_ident(namespace, table_name)?;
        let table = self.inner.load_table(&ident).await.map_err(|e| {
            IcebergStorageError::Catalog(format!("Glue load table handle failed: {}", e))
        })?;
        Ok(table)
    }

    async fn commit_transaction(&self, tx: Transaction) -> Result<Table> {
        let table = tx
            .commit(&self.inner)
            .await
            .map_err(|e| IcebergStorageError::Catalog(e.to_string()))?;
        Ok(table)
    }
}
