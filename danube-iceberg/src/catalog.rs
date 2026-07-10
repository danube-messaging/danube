//! Iceberg catalog builder — dispatches to the configured catalog backend.
//!
//! All 4 catalog implementations (REST, Glue, S3Tables, SQL) are compiled into
//! the binary. The user selects which one to use via the `catalog.type` config
//! field. This avoids feature-flag complexity while keeping the binary ergonomic.
//!
//! For testing, the `iceberg` core crate provides `MemoryCatalog` — see the
//! integration tests.

use crate::config::CatalogConfig;
use iceberg::Catalog;
use std::sync::Arc;
use tracing::info;

/// Build an Iceberg catalog from the config, dispatching to the right backend.
///
/// Returns `Arc<dyn Catalog>` — the trait object is catalog-agnostic, so all
/// downstream code (table creation, data file commits, schema evolution) works
/// identically regardless of which backend is configured.
pub async fn build_catalog(config: &CatalogConfig) -> anyhow::Result<Arc<dyn Catalog>> {
    info!(
        catalog_type = %config.type_,
        catalog_name = %config.name,
        "building iceberg catalog"
    );

    let catalog: Arc<dyn Catalog> = match config.type_.as_str() {
        "rest" => {
            use iceberg::CatalogBuilder;
            use iceberg_catalog_rest::RestCatalogBuilder;
            let cat = RestCatalogBuilder::default()
                .load(&config.name, config.properties.clone())
                .await
                .map_err(|e| anyhow::anyhow!("failed to build REST catalog: {}", e))?;
            Arc::new(cat)
        }
        "glue" => {
            use iceberg::CatalogBuilder;
            use iceberg_catalog_glue::GlueCatalogBuilder;
            let cat = GlueCatalogBuilder::default()
                .load(&config.name, config.properties.clone())
                .await
                .map_err(|e| anyhow::anyhow!("failed to build Glue catalog: {}", e))?;
            Arc::new(cat)
        }
        "s3tables" => {
            use iceberg::CatalogBuilder;
            use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
            let cat = S3TablesCatalogBuilder::default()
                .load(&config.name, config.properties.clone())
                .await
                .map_err(|e| anyhow::anyhow!("failed to build S3Tables catalog: {}", e))?;
            Arc::new(cat)
        }
        "sql" => {
            use iceberg::CatalogBuilder;
            use iceberg_catalog_sql::SqlCatalogBuilder;
            let cat = SqlCatalogBuilder::default()
                .load(&config.name, config.properties.clone())
                .await
                .map_err(|e| anyhow::anyhow!("failed to build SQL catalog: {}", e))?;
            Arc::new(cat)
        }
        other => anyhow::bail!(
            "unsupported catalog type: '{}' (supported: rest, glue, s3tables, sql)",
            other
        ),
    };

    info!(catalog_type = %config.type_, "iceberg catalog ready");
    Ok(catalog)
}
