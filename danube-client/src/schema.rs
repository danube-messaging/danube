use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use danube_core::proto::{schema::TypeSchema as ProtoTypeSchema, Schema as ProtoSchema};

/// Represents a schema for data, including its type and associated schema data.
///
/// This struct is used to define how data should be serialized, deserialized, and validated.
///
/// Fields:
/// - `name`: The name of the schema, typically used for identification purposes.
/// - `schema_data`: The schema data itself, which contains the schema's definition. This is only used when `type_schema` is `Json`.
/// - `type_schema`: The type of schema that determines the format of the data (e.g., JSON, STRING).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub schema_data: Option<Vec<u8>>,
    pub type_schema: SchemaType,
}

impl Schema {
    pub fn new(name: String, type_schema: SchemaType) -> Self {
        let schema_data = match &type_schema {
            SchemaType::Json(schema) => Some(schema.as_bytes().to_vec()),
            _ => None,
        };
        Self {
            name,
            schema_data,
            type_schema,
        }
    }
}

/// Represents the type of schema used for data serialization and validation.
///
/// This enum defines the possible types of schemas that can be applied to data.
///
/// Variants:
/// - `Bytes`: Represents a schema where data is in raw bytes format.
/// - `String`: Represents a schema where data is in string format.
/// - `Int64`: Represents a schema where data is in 64-bit integer format.
/// - `Json`: Represents a schema where data is in JSON format. The associated string holds the JSON schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaType {
    Bytes,
    String,
    Int64,
    Json(String), // JSON schema described by a string
}

// Implement conversions from SchemaType to ProtoTypeSchema
impl From<SchemaType> for ProtoTypeSchema {
    fn from(schema_type: SchemaType) -> Self {
        match schema_type {
            SchemaType::Bytes => ProtoTypeSchema::Bytes,
            SchemaType::String => ProtoTypeSchema::String,
            SchemaType::Int64 => ProtoTypeSchema::Int64,
            SchemaType::Json(_) => ProtoTypeSchema::Json,
        }
    }
}

// Implement conversions from ProtoTypeSchema to SchemaType
impl From<ProtoTypeSchema> for SchemaType {
    fn from(proto_schema: ProtoTypeSchema) -> Self {
        match proto_schema {
            ProtoTypeSchema::Bytes => SchemaType::Bytes,
            ProtoTypeSchema::String => SchemaType::String,
            ProtoTypeSchema::Int64 => SchemaType::Int64,
            ProtoTypeSchema::Json => SchemaType::Json(String::new()),
        }
    }
}

// Implement a conversion method from ProtoSchema to Schema
impl From<ProtoSchema> for Schema {
    fn from(proto_schema: ProtoSchema) -> Self {
        let type_schema =
            ProtoTypeSchema::try_from(proto_schema.type_schema).expect("Invalid type schema");
        Schema {
            name: proto_schema.name,
            schema_data: Some(proto_schema.schema_data),
            type_schema: type_schema.into(),
        }
    }
}

// Implement a conversion method from Schema to ProtoSchema
impl From<Schema> for ProtoSchema {
    fn from(schema: Schema) -> Self {
        ProtoSchema {
            name: schema.name,
            schema_data: schema.schema_data.unwrap_or_default(),
            type_schema: ProtoTypeSchema::from(schema.type_schema).into(),
        }
    }
}
