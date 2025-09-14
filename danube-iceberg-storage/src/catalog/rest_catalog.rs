use crate::errors::{IcebergStorageError, Result};
use async_trait::async_trait;

// Import the types from the parent catalog module
use super::IcebergCatalog;

// Official Iceberg crates
use iceberg::spec::Schema as IcebergSchema;
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::NamespaceIdent;
use iceberg::{Catalog as IcebergCatalogTrait, TableIdent};
use iceberg_catalog_rest as rest;

/// REST catalog wrapper implementation using the official Iceberg REST catalog
#[derive(Debug)]
pub struct RestCatalog {
    inner: rest::RestCatalog,
    warehouse: String,
}

impl RestCatalog {
    pub async fn new(uri: &str, warehouse: &str) -> Result<Self> {
        let cfg = rest::RestCatalogConfig::builder()
            .uri(uri.to_string())
            .warehouse(warehouse.to_string())
            .build()
            .map_err(|e| {
                IcebergStorageError::Catalog(format!("Failed to build REST catalog config: {}", e))
            })?;
        let inner = rest::RestCatalog::new(cfg);
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
impl IcebergCatalog for RestCatalog {
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
            .map_err(|e| IcebergStorageError::Catalog(format!("Create table failed: {}", e)))?;
        self.inner
            .create_table(&ns, creation)
            .await
            .map_err(|e| IcebergStorageError::Catalog(format!("Create table failed: {}", e)))?;

        Ok(())
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
            .create_namespace(&ns, std::collections::HashMap::new())
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

    async fn load_table_handle(&self, namespace: &str, table_name: &str) -> Result<Table> {
        let ident = Self::to_ident(namespace, table_name)?;
        let table = self.inner.load_table(&ident).await.map_err(|e| {
            IcebergStorageError::Catalog(format!("REST load table handle failed: {}", e))
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
