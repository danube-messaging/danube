//! Iceberg table lifecycle management.
//!
//! Handles creating, loading, and committing data files to Iceberg tables.
//! Each topic maps to one Iceberg table. The `TableManager` ensures the table
//! exists (creating it if needed) and commits Parquet data files after each flush.
//!
//! ## Commit flow
//!
//! ```text
//! 1. writer.rs produces a Parquet file in object storage
//! 2. TableManager builds a DataFile descriptor (path, format, row count, size)
//! 3. Transaction::new(table).fast_append().add_data_files([data_file])
//! 4. transaction.commit(&catalog) — atomic metadata update
//! ```

use crate::iceberg_schema;
use arrow_schema::Schema as ArrowSchema;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Manages Iceberg tables for topic-to-table mapping.
pub struct TableManager {
    catalog: Arc<dyn Catalog>,
}

impl TableManager {
    /// Create a new TableManager with the given catalog.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Get or create the Iceberg table for a topic.
    ///
    /// - If the table exists in the catalog, load it.
    /// - If not, create it with the schema derived from the Arrow schema.
    /// - Also ensures the namespace exists.
    pub async fn get_or_create_table(
        &self,
        namespace: &str,
        table_name: &str,
        arrow_schema: &ArrowSchema,
    ) -> anyhow::Result<iceberg::table::Table> {
        let ns = NamespaceIdent::from_strs([namespace])
            .map_err(|e| anyhow::anyhow!("invalid namespace '{}': {}", namespace, e))?;

        let table_ident = TableIdent::new(ns.clone(), table_name.to_string());

        // Try to load existing table
        match self.catalog.load_table(&table_ident).await {
            Ok(table) => {
                debug!(namespace = %namespace, table = %table_name, "loaded existing iceberg table");
                Ok(table)
            }
            Err(_) => {
                // Table doesn't exist — create namespace if needed, then create table
                info!(
                    namespace = %namespace,
                    table = %table_name,
                    "iceberg table not found, creating"
                );

                // Ensure namespace exists (idempotent — ignore "already exists" errors)
                if let Err(e) = self
                    .catalog
                    .create_namespace(&ns, Default::default())
                    .await
                {
                    debug!(
                        namespace = %namespace,
                        error = %e,
                        "namespace create returned error (may already exist)"
                    );
                }

                // Convert Arrow schema → Iceberg schema
                let iceberg_schema = iceberg_schema::arrow_to_iceberg_schema(arrow_schema)?;

                // Create the table
                let table_creation = TableCreation::builder()
                    .name(table_name.to_string())
                    .schema(iceberg_schema)
                    .build();

                let table = self
                    .catalog
                    .create_table(&ns, table_creation)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "failed to create iceberg table '{}/{}': {}",
                            namespace,
                            table_name,
                            e
                        )
                    })?;

                info!(
                    namespace = %namespace,
                    table = %table_name,
                    "created iceberg table"
                );

                Ok(table)
            }
        }
    }

    /// Commit a Parquet data file to an Iceberg table via fast_append.
    ///
    /// This is the core integration point: after `writer.rs` produces a Parquet
    /// file, we register it with the Iceberg catalog so it appears in table scans.
    pub async fn commit_data_file(
        &self,
        table: &iceberg::table::Table,
        data_file_path: &str,
        record_count: u64,
        file_size_bytes: u64,
    ) -> anyhow::Result<iceberg::table::Table> {
        // Build the DataFile descriptor
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(data_file_path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(record_count)
            .file_size_in_bytes(file_size_bytes)
            .partition(Struct::empty())
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build DataFile: {}", e))?;

        debug!(
            path = %data_file_path,
            records = record_count,
            size_bytes = file_size_bytes,
            "committing data file to iceberg"
        );

        // Transaction: fast_append → commit
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(vec![data_file]);
        let tx = action
            .apply(tx)
            .map_err(|e| anyhow::anyhow!("fast_append apply failed: {}", e))?;

        let updated_table = tx
            .commit(&*self.catalog)
            .await
            .map_err(|e| anyhow::anyhow!("iceberg commit failed: {}", e))?;

        info!(
            path = %data_file_path,
            records = record_count,
            "committed data file to iceberg table"
        );

        Ok(updated_table)
    }

    /// Check if the table schema needs evolution (new fields) and apply if needed.
    ///
    /// Iceberg supports additive schema evolution — we can add new columns without
    /// breaking existing data. This is called when the inferred JSON schema changes
    /// (e.g., a producer adds a new field to their JSON payloads).
    #[allow(dead_code)] // Prepared for schema evolution — will be called from worker.rs
    pub async fn evolve_schema_if_needed(
        &self,
        table: &iceberg::table::Table,
        new_arrow_schema: &ArrowSchema,
    ) -> anyhow::Result<bool> {
        let new_iceberg_schema = iceberg_schema::arrow_to_iceberg_schema(new_arrow_schema)?;
        let existing_schema = table.metadata().current_schema();

        match iceberg_schema::schema_diff(existing_schema, &new_iceberg_schema) {
            Ok(new_fields) => {
                if new_fields.is_empty() {
                    return Ok(false); // No evolution needed
                }

                warn!(
                    new_fields = new_fields.len(),
                    "schema evolution detected — new fields will be added on next table creation"
                );

                // Note: Full schema evolution via Transaction is complex in iceberg-rs 0.9.1.
                // For now, we log the diff. The next table recreation will pick up new fields.
                // TODO: Implement Transaction-based schema evolution when iceberg-rs API stabilizes.
                Ok(true)
            }
            Err(e) => {
                warn!(error = %e, "incompatible schema change detected");
                Err(e)
            }
        }
    }
}
