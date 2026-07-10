//! Iceberg table lifecycle management.
//!
//! Handles creating, loading, and committing data files to Iceberg tables.
//! Each topic maps to one Iceberg table. The `TableManager` ensures the table
//! exists (creating it if needed) and commits data files after each flush.
//!
//! ## Commit flow
//!
//! ```text
//! 1. writer.rs produces DataFile(s) via iceberg's writer pipeline
//!    (ParquetWriterBuilder → RollingFileWriter → DataFileWriter)
//!    with full per-column statistics (min/max, nulls, sizes)
//! 2. Transaction::new(table).fast_append().add_data_files(data_files)
//! 3. transaction.commit(&catalog) — atomic metadata update
//! ```

use crate::iceberg_schema;
use arrow_schema::Schema as ArrowSchema;
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

    /// Commit data files to an Iceberg table via fast_append.
    ///
    /// The `DataFile` instances are produced by iceberg's writer pipeline
    /// and already contain full per-column statistics (min/max bounds,
    /// null counts, value counts, column sizes).
    ///
    /// ## Commit ordering
    ///
    /// This is called *after* the data files are written to storage but
    /// *before* saving the checkpoint — ensuring at-least-once semantics.
    pub async fn commit_data_files(
        &self,
        table: &iceberg::table::Table,
        data_files: Vec<iceberg::spec::DataFile>,
    ) -> anyhow::Result<iceberg::table::Table> {
        let total_records: u64 = data_files.iter().map(|f| f.record_count()).sum();
        let file_count = data_files.len();

        debug!(
            files = file_count,
            records = total_records,
            "committing data files to iceberg"
        );

        // Transaction: fast_append → commit
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action
            .apply(tx)
            .map_err(|e| anyhow::anyhow!("fast_append apply failed: {}", e))?;

        let updated_table = tx
            .commit(&*self.catalog)
            .await
            .map_err(|e| anyhow::anyhow!("iceberg commit failed: {}", e))?;

        info!(
            files = file_count,
            records = total_records,
            "committed data files to iceberg table"
        );

        Ok(updated_table)
    }

    /// Check if the table schema needs evolution (new fields) and report.
    ///
    /// Iceberg supports additive schema evolution (adding nullable columns
    /// without rewriting data), but `iceberg-rust 0.9.1` does not expose
    /// `update_schema()` on `Transaction`. This method performs detection
    /// only — it compares the existing table schema against the new Arrow
    /// schema and logs the result.
    ///
    /// When `iceberg-rust` adds schema evolution to its Transaction API,
    /// this method should be updated to apply the change atomically.
    ///
    /// Returns `Ok(true)` if new fields were detected, `Ok(false)` if
    /// schemas are identical, or `Err` on incompatible type changes.
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

                let field_names: Vec<_> = new_fields.iter().map(|f| f.name.as_str()).collect();
                warn!(
                    new_fields = new_fields.len(),
                    field_names = ?field_names,
                    "schema evolution detected — new columns found but iceberg-rust 0.9.1 \
                     does not support Transaction::update_schema(). New Parquet files will \
                     contain the additional columns; the Iceberg table schema will be updated \
                     when the crate adds this capability."
                );

                Ok(true)
            }
            Err(e) => {
                warn!(error = %e, "incompatible schema change detected — rejecting");
                Err(e)
            }
        }
    }
}
