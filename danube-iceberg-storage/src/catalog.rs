use crate::config::CatalogConfig;
use crate::errors::{IcebergStorageError, Result};
use std::sync::Arc;

// Official Iceberg crates
use iceberg::io::FileIOBuilder;
use iceberg::memory::MemoryCatalog as IcebergMemoryCatalog;
use iceberg::spec::{PrimitiveType, Schema as IcebergSchema, Type as IcebergType};
use iceberg::Catalog as IcebergCatalogTrait;

// External catalog crates
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

/// Create catalog based on configuration, returning the official Iceberg trait object
pub async fn create_catalog(
    config: &CatalogConfig,
    warehouse: &str,
) -> Result<Arc<dyn IcebergCatalogTrait>> {
    match config {
        CatalogConfig::Rest { uri, .. } => {
            let cfg = RestCatalogConfig::builder()
                .uri(uri.clone())
                .warehouse(warehouse.to_string())
                .build();
            let catalog = RestCatalog::new(cfg);
            Ok(Arc::new(catalog))
        }
        CatalogConfig::Glue {
            region: _region,
            database: _database,
            ..
        } => {
            let cfg = GlueCatalogConfig::builder()
                .warehouse(warehouse.to_string())
                .build();
            let catalog = GlueCatalog::new(cfg).await.map_err(|e| {
                IcebergStorageError::Catalog(format!("Failed to create Glue catalog: {}", e))
            })?;
            Ok(Arc::new(catalog))
        }
        CatalogConfig::Memory {} => {
            let io = FileIOBuilder::new("memory").build().map_err(|e| {
                IcebergStorageError::Catalog(format!("Failed to build memory FileIO: {}", e))
            })?;
            let catalog = IcebergMemoryCatalog::new(io, None);
            Ok(Arc::new(catalog))
        }
    }
}

/// Build the standard Danube message schema directly as an IcebergSchema
pub fn create_danube_schema() -> IcebergSchema {
    use iceberg::spec::NestedField;
    let mut fields: Vec<std::sync::Arc<NestedField>> = Vec::new();

    fields.push(
        NestedField::required(1, "request_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
    );
    fields.push(
        NestedField::required(
            2,
            "producer_id",
            IcebergType::Primitive(PrimitiveType::Long),
        )
        .into(),
    );
    fields.push(
        NestedField::required(
            3,
            "topic_name",
            IcebergType::Primitive(PrimitiveType::String),
        )
        .into(),
    );
    fields.push(
        NestedField::required(
            4,
            "broker_addr",
            IcebergType::Primitive(PrimitiveType::String),
        )
        .into(),
    );
    fields.push(
        NestedField::required(5, "segment_id", IcebergType::Primitive(PrimitiveType::Long)).into(),
    );
    fields.push(
        NestedField::required(
            6,
            "segment_offset",
            IcebergType::Primitive(PrimitiveType::Long),
        )
        .into(),
    );
    fields.push(
        NestedField::required(7, "payload", IcebergType::Primitive(PrimitiveType::Binary)).into(),
    );
    fields.push(
        NestedField::required(
            8,
            "publish_time",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .into(),
    );
    fields.push(
        NestedField::required(
            9,
            "producer_name",
            IcebergType::Primitive(PrimitiveType::String),
        )
        .into(),
    );
    fields.push(
        NestedField::optional(
            10,
            "subscription_name",
            IcebergType::Primitive(PrimitiveType::String),
        )
        .into(),
    );

    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .expect("build danube schema")
}
