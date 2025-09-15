use crate::config::{CatalogConfig, ObjectStoreConfig};
use crate::errors::{IcebergStorageError, Result};
use crate::warehouse::{create_catalog_properties, create_file_io};
use std::sync::Arc;

// Official Iceberg crates
use iceberg::memory::MemoryCatalog as IcebergMemoryCatalog;
use iceberg::spec::{PrimitiveType, Schema as IcebergSchema, Type as IcebergType};
use iceberg::Catalog as IcebergCatalogTrait;

// External catalog crates
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

/// Create catalog based on configuration, returning the official Iceberg trait object
///
/// Note: The FileIO configuration is handled differently for each catalog type:
/// - REST/Glue: Use warehouse path scheme + properties (catalogs create FileIO internally)
/// - Memory: Explicit FileIO creation (follows MemoryCatalog::new() pattern)
///
/// Warehouse path examples:
/// - s3://bucket/path -> S3 FileIO with properties from config + environment
/// - file:///path -> Local filesystem FileIO
/// - gs://bucket/path -> GCS FileIO with properties from config + environment
/// - memory://path -> Memory FileIO for testing
pub async fn create_catalog(
    catalog_config: &CatalogConfig,
    object_store_config: &ObjectStoreConfig,
    warehouse: &str,
) -> Result<Arc<dyn IcebergCatalogTrait>> {
    match catalog_config {
        CatalogConfig::Rest { uri, .. } => {
            // REST catalog creates FileIO internally based on warehouse path + properties
            let props = create_catalog_properties(object_store_config);

            let cfg = RestCatalogConfig::builder()
                .uri(uri.clone())
                .warehouse(warehouse.to_string())
                .props(props)
                .build();
            let catalog = RestCatalog::new(cfg);
            Ok(Arc::new(catalog))
        }
        CatalogConfig::Glue {
            region: _region,
            database: _database,
            ..
        } => {
            // Glue catalog creates FileIO internally based on warehouse path + properties
            let props = create_catalog_properties(object_store_config);

            let cfg = GlueCatalogConfig::builder()
                .warehouse(warehouse.to_string())
                .props(props)
                .build();
            let catalog = GlueCatalog::new(cfg).await.map_err(|e| {
                IcebergStorageError::Catalog(format!("Failed to create Glue catalog: {}", e))
            })?;
            Ok(Arc::new(catalog))
        }
        CatalogConfig::Memory {} => {
            // Memory catalog requires explicit FileIO creation
            // This follows the same pattern as MemoryCatalog::new() in Iceberg source
            tracing::debug!(
                "Creating MemoryCatalog with warehouse: {}, object_store_config: {:?}",
                warehouse,
                object_store_config
            );

            let file_io = create_file_io(object_store_config, warehouse).await?;

            tracing::debug!("FileIO created successfully, initializing MemoryCatalog");

            let catalog = IcebergMemoryCatalog::new(file_io, None);

            tracing::debug!("MemoryCatalog created successfully");

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
